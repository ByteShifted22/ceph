// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GroupDisableRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/api/Image.h"
#include "librbd/api/Mirror.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/mirror/snapshot/Utils.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/mirror/GroupGetInfoRequest.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/mirror/GroupPromoteRequest.h"
#include "librbd/mirror/ImageRemoveRequest.h"
#include "librbd/mirror/ImageStateUpdateRequest.h"
#include "librbd/mirror/snapshot/PromoteRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"
#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"


#include <shared_mutex>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupDisableRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
void GroupDisableRequest<I>::send() {
  get_mirror_group_info();
}

template <typename I>
void GroupDisableRequest<I>::get_mirror_group_info() {
  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_get_mirror_group_info>(this);

  auto request = mirror::GroupGetInfoRequest<I>::create(
    m_group_ioctx, "", m_group_id, &m_mirror_group, &m_promotion_state, ctx);
  request->send();
}

template <typename I>
void GroupDisableRequest<I>::handle_get_mirror_group_info(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;
  if (r == -ENOENT) {
    ldout(m_cct, 10) << "ignoring disable command: mirroring is not enabled for "
                     << "this group: " << m_group_name << dendl;
    finish(0);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to get mirror group info: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_is_group_primary = (m_promotion_state == mirror::PROMOTION_STATE_PRIMARY ||
                        m_promotion_state == mirror::PROMOTION_STATE_UNKNOWN);

  if (!m_is_group_primary && !m_force) {
    lderr(m_cct) << "mirrored group " << m_group_name
                 << " is not primary, add force option to disable mirroring"
                 << dendl;
    finish(-EINVAL);
    return;
  }

  check_group_resync_flagged();
}

template <typename I>
void GroupDisableRequest<I>::check_group_resync_flagged() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::metadata_get_start(&op, RBD_GROUP_RESYNC);

  m_out_bl.clear();

  std::string group_header_oid = librbd::util::group_header_name(m_group_id);
  auto aio_comp = create_rados_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_check_group_resync_flagged>(this);

  int r = m_group_ioctx.aio_operate(group_header_oid, aio_comp,
                                    &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupDisableRequest<I>::handle_check_group_resync_flagged(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::string data;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::metadata_get_finish(&it, &data);
  }
  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed reading metadata: " << RBD_GROUP_RESYNC << " : "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (r == 0) {
    ldout(m_cct, 10) << "found previous resync group request, clearing it"
                     << dendl;
    remove_resync_key();
    return;
  }

  prepare_group_images();
}

template <typename I>
void GroupDisableRequest<I>::remove_resync_key() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::metadata_remove(&op, RBD_GROUP_RESYNC);

  auto aio_comp = create_rados_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_remove_resync_key>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupDisableRequest<I>::handle_remove_resync_key(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed removing group resync key: " << RBD_GROUP_RESYNC
                 << " : " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  prepare_group_images();
}

template <typename I>
void GroupDisableRequest<I>::prepare_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_prepare_group_images>(this);

  auto req = snapshot::GroupPrepareImagesRequest<I>::create(m_group_ioctx,
    m_group_id, m_image_ctxs, m_images, &m_mirror_images, nullptr,
    snapshot::GroupPrepareImagesRequest<I>::OP_DISABLE, m_force, ctx);
  req->send();
}

template <typename I>
void GroupDisableRequest<I>::handle_prepare_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to prepare group images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    set_images_read_only_mask_non_primary();
    return;
  }
  ceph_assert(m_image_ctxs.size() == m_images.size());
  ceph_assert(m_image_ctxs.size() == m_mirror_images.size());

  if (m_images.empty()) {
    if (!m_is_group_primary) {
      force_promote_group();
    } else {
      set_mirror_group_disabling();
    }
  } else {
    get_images_mirror_uuid();
  }
}

template <typename I>
void GroupDisableRequest<I>::get_images_mirror_uuid() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_get_images_mirror_uuid>(this);

  m_image_mirror_uuid.resize(m_image_ctxs.size());
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    auto uuid_ctx = new LambdaContext(
      [new_sub_ctx=gather_ctx->new_sub()](int r) {
        if (r == -ENOENT) {
          r = 0;
        }
        new_sub_ctx->complete(r);
        return;
    });
    api::Mirror<I>::uuid_get(m_image_ctxs[i]->md_ctx,
          &m_image_mirror_uuid[i], uuid_ctx);
  }
  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_get_images_mirror_uuid(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to get mirror_uuid" << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    set_images_read_only_mask_non_primary();
    return;
  }
  remove_snapshot_keys();
}

template <typename I>
void GroupDisableRequest<I>::remove_snapshot_keys() {
  ldout(m_cct, 10) << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    int r = m_image_ctxs[i]->operations->metadata_remove(
      mirror::snapshot::util::get_image_meta_key(m_image_mirror_uuid[i]));
    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "cannot remove snapshot image-meta key: " << cpp_strerror(r)
                   << dendl;
      m_ret_val = r;
      set_images_read_only_mask_non_primary();
      return;
    }
  }

  if (!m_is_group_primary) {
    force_promote_group();
  } else {
    set_mirror_group_disabling();
  }
}

template <typename I>
void GroupDisableRequest<I>::force_promote_group() {
  // If the group is not primary, promote it to primary.
  // This allows removal of non-primary member images by
  // draining their watchers and removing their non-primary feature flag.
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_force_promote_group>(this);

  auto req = mirror::GroupPromoteRequest<>::create(
    m_group_ioctx, m_group_id, m_group_name, true, ctx);
  req->send();
}

template <typename I>
void GroupDisableRequest<I>::handle_force_promote_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    m_ret_val = r;
    lderr(m_cct) << "failed to force promote group: "
                 << cpp_strerror(r) << dendl;
    set_images_read_only_mask_non_primary();
    return;
  }

  refresh_images();
}

template <typename I>
void GroupDisableRequest<I>::refresh_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_refresh_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    if (m_image_ctxs[i]->state->is_refresh_required()) {
      m_image_ctxs[i]->state->refresh(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_refresh_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to refresh images: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    set_images_read_only_mask_non_primary();
    return;
  }

  set_mirror_group_disabling();
}

template <typename I>
void GroupDisableRequest<I>::set_mirror_group_disabling() {
  ldout(m_cct, 10) << dendl;

  m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLING;

  librados::ObjectWriteOperation op;
  cls_client::mirror_group_set(&op, m_group_id, m_mirror_group);
  auto aio_comp = create_rados_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_set_mirror_group_disabling>(this);
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupDisableRequest<I>::handle_set_mirror_group_disabling(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to set mirror group as disabling: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    set_images_read_only_mask_non_primary();
    return;
  }

  if (m_images.empty()) {
    list_group_snapshots();
  } else {
    set_mirror_images_disabling();
  }
}

template <typename I>
void GroupDisableRequest<I>::set_mirror_images_disabling() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_set_mirror_images_disabling>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    auto req = ImageStateUpdateRequest<I>::create(
      m_image_ctxs[i]->md_ctx, m_image_ctxs[i]->id,
      cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
      m_mirror_images[i], gather_ctx->new_sub());

    req->send();
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_set_mirror_images_disabling(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to disable mirror images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    set_images_read_only_mask_non_primary();
    return;
  }

  list_group_snapshots();
}

template <typename I>
void GroupDisableRequest<I>::list_group_snapshots() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>, &GroupDisableRequest<I>::handle_list_group_snapshots>(this);
  auto req = group::ListSnapshotsRequest<>::create(m_group_ioctx, m_group_id,
                                                   true, true, &m_group_snaps, ctx);
  req->send();
}

template <typename I>
void GroupDisableRequest<I>::handle_list_group_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to list snapshots for group: "
                 << m_group_name << dendl;
    m_ret_val = r;
    set_images_read_only_mask_non_primary();
    return;
  }
  // removes group mirror snapshots and
  // their corresponding images snapshots
  remove_group_mirror_snaps();
}

template <typename I>
void GroupDisableRequest<I>::remove_group_mirror_snaps() {
  ldout(m_cct, 10) << dendl;

  m_group_snap_index = 0;
  remove_group_mirror_snapshot();
}

template <typename I>
void GroupDisableRequest<I>::remove_group_mirror_snapshot() {
  while (m_group_snap_index < m_group_snaps.size()) {
    auto& group_snap = m_group_snaps[m_group_snap_index];
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &group_snap.snapshot_namespace);
    if (ns != nullptr) {
      break;
    }
    // Skip non-mirror snapshots
    ++m_group_snap_index;
  }

  if (m_group_snap_index >= m_group_snaps.size()) {
    // All mirror group snapshots removed
    remove_mirror_images();
    return;
  }

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_remove_group_mirror_snapshot>(this);

  ldout(m_cct, 10) << "removing group mirror snapshot with group_snap_id="
                   << m_group_snaps[m_group_snap_index].id << dendl;
  auto req = snapshot::RemoveGroupSnapshotRequest<I>::create(
    m_group_ioctx, m_group_id, &m_group_snaps[m_group_snap_index],
    &m_image_ctxs, ctx);
  req->send();
}

template <typename I>
void GroupDisableRequest<I>::handle_remove_group_mirror_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove group mirror snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    set_images_read_only_mask_non_primary();
    return;
  }

  // Successfully removed this snapshot
  // Procced to removal of next mirror group snapshot
  ++m_group_snap_index;
  remove_group_mirror_snapshot();
}

template <typename I>
void GroupDisableRequest<I>::remove_mirror_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_remove_mirror_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    auto req = ImageRemoveRequest<I>::create(
      m_image_ctxs[i]->md_ctx, m_mirror_images[i].global_image_id,
      m_image_ctxs[i]->id, gather_ctx->new_sub());
    req->send();
  }
  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_remove_mirror_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    set_images_read_only_mask_non_primary();
    return;
  }

  remove_mirror_group();
}

template <typename I>
void GroupDisableRequest<I>::remove_mirror_group() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_group_remove(&op, m_group_id);

  auto aio_comp = create_rados_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_remove_mirror_group>(this);

  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupDisableRequest<I>::handle_remove_mirror_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirroring group: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  notify_mirroring_watcher();
}

template <typename I>
void GroupDisableRequest<I>::notify_mirroring_watcher() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_notify_mirroring_watcher>(this);

  librbd::MirroringWatcher<I>::notify_group_updated(
    m_group_ioctx, cls::rbd::MIRROR_GROUP_STATE_DISABLED, m_group_id,
    m_mirror_group.global_group_id, m_images.size(), ctx);
}

template <typename I>
void GroupDisableRequest<I>::handle_notify_mirroring_watcher(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to notify mirroring group=" << m_group_name
                 << " updated: " << cpp_strerror(r) << dendl;
  }

  close_images();
}

template <typename I>
void GroupDisableRequest<I>::close_images() {
  if (m_image_ctxs.empty()) {
    finish(m_ret_val);
    return;
  }
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>, &GroupDisableRequest<I>::handle_close_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (auto ictx: m_image_ctxs) {
    if (ictx != nullptr) {
      ictx->state->close(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_close_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to close images: " << cpp_strerror(r) << dendl;
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
  }

  finish(m_ret_val);
}

template <typename I>
void GroupDisableRequest<I>::set_images_read_only_mask_non_primary() {
  if (m_image_ctxs.empty()) {
    finish(m_ret_val);
    return;
  }
  ldout(m_cct, 10) << dendl;

  for (auto ictx : m_image_ctxs) {
    if (ictx == nullptr) {
      continue;
    }
    {
      std::lock_guard locker{ictx->image_lock};
      ictx->read_only_mask |= IMAGE_READ_ONLY_FLAG_NON_PRIMARY;
    }
    ictx->state->handle_update_notification();
  }

  close_images();
}

template <typename I>
void GroupDisableRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupDisableRequest<librbd::ImageCtx>;
