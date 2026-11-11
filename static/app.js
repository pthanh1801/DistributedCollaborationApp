let currentTarget = '*';
let userScrolledUp = false;
let myUsername = '';
let lastState = null;
let lastTimelineSignature = '';
let pendingFile = null;
let pendingUploads = [];
let pollTimer = null;
let dragDepth = 0;

const msgContainer = document.getElementById('messages');
const scrollBottomBtn = document.getElementById('scroll-bottom-btn');

function updateScrollBottomButton() {
  if (!msgContainer || !scrollBottomBtn) return;
  const hasOverflow = msgContainer.scrollHeight > msgContainer.clientHeight + 20;
  scrollBottomBtn.style.display = hasOverflow && userScrolledUp ? 'inline-flex' : 'none';
}

function scrollToBottom() {
  if (!msgContainer) return;
  userScrolledUp = false;
  msgContainer.scrollTo({ top: msgContainer.scrollHeight, behavior: 'smooth' });
  updateScrollBottomButton();
}

if (msgContainer) {
  msgContainer.addEventListener('scroll', () => {
    const isNearBottom = msgContainer.scrollHeight - msgContainer.scrollTop - msgContainer.clientHeight < 60;
    userScrolledUp = !isNearBottom;
    updateScrollBottomButton();
  });
}

const textInput = document.getElementById('text');
if (textInput) {
  textInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      if (textInput.value.trim()) {
        sendMessage();
      } else if (pendingFile) {
        sendFile();
      }
    }
  });
}

// Drag & drop support inside chat panel
const chatPanel = document.getElementById('panel-chat');
if (chatPanel) {
  chatPanel.addEventListener('dragenter', (e) => {
    e.preventDefault();
    dragDepth += 1;
    chatPanel.classList.add('drag-over');
    const overlayText = document.getElementById('drop-overlay-text');
    if (overlayText) {
      overlayText.textContent = currentTarget === '*'
        ? 'Thả tệp vào đây để gửi cho Mọi người (Broadcast)'
        : `Thả tệp vào đây để gửi riêng cho ${currentTarget}`;
    }
  });

  chatPanel.addEventListener('dragover', (e) => {
    e.preventDefault();
  });

  chatPanel.addEventListener('dragleave', (e) => {
    e.preventDefault();
    dragDepth = Math.max(0, dragDepth - 1);
    if (dragDepth === 0) {
      chatPanel.classList.remove('drag-over');
    }
  });

  chatPanel.addEventListener('drop', (e) => {
    e.preventDefault();
    dragDepth = 0;
    chatPanel.classList.remove('drag-over');
    if (e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      setSelectedFile(e.dataTransfer.files[0]);
    }
  });
}

function formatBytes(bytes) {
  if (typeof bytes !== 'number' || Number.isNaN(bytes)) return '';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
}

function setSelectedFile(file) {
  const infoBox = document.getElementById('selected-file-box');
  if (!file) {
    pendingFile = null;
    if (infoBox) infoBox.style.display = 'none';
    return;
  }
  pendingFile = file;
  const nameElem = document.getElementById('selected-file-name');
  const sizeElem = document.getElementById('selected-file-size');
  if (nameElem) nameElem.textContent = file.name;
  if (sizeElem) sizeElem.textContent = formatBytes(file.size);
  if (infoBox) infoBox.style.display = 'flex';
}

function handleFileSelect(input) {
  if (input.files && input.files.length > 0) {
    setSelectedFile(input.files[0]);
  } else {
    setSelectedFile(null);
  }
}

function clearSelectedFile() {
  const input = document.getElementById('file');
  if (input) input.value = '';
  setSelectedFile(null);
}

function selectTarget(target) {
  currentTarget = target;
  const targetSelect = document.getElementById('target');
  if (targetSelect) {
    targetSelect.value = target;
  }

  const title = document.getElementById('chat-target-title');
  const typeTag = document.getElementById('chat-target-type');
  const avatar = document.getElementById('chat-target-avatar');

  if (target === '*') {
    if (title) title.textContent = 'Mọi người (Broadcast)';
    if (typeTag) {
      typeTag.textContent = 'Phòng chat chung';
      typeTag.className = 'chat-recipient-tag';
    }
    if (avatar) {
      avatar.textContent = '*';
      avatar.className = 'user-avatar broadcast-avatar';
    }
  } else {
    if (title) title.textContent = target;
    if (typeTag) {
      typeTag.textContent = 'Tin nhắn riêng (Unicast)';
      typeTag.className = 'chat-recipient-tag private';
    }
    if (avatar) {
      avatar.textContent = target.charAt(0).toUpperCase();
      avatar.className = 'user-avatar';
    }
  }

  userScrolledUp = false;
  lastTimelineSignature = '';
  if (lastState) {
    renderState(lastState);
  }
  refresh();
}

async function api(path, options) {
  const r = await fetch(path, options);
  const result = await r.json();
  if (!r.ok) throw new Error(result.error || 'Yêu cầu không thành công');
  return result;
}

function showNotice(text, isSuccess = false) {
  const toast = document.getElementById('notice-toast');
  const toastText = document.getElementById('notice-toast-text');
  if (!toast || !toastText) return;
  toastText.textContent = text;
  toast.className = isSuccess ? 'success' : '';
  toast.style.display = 'flex';
  setTimeout(() => {
    toast.style.display = 'none';
  }, 4000);
}

function hideNotice() {
  const toast = document.getElementById('notice-toast');
  if (toast) toast.style.display = 'none';
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[c]));
}

function belongsToRoom(item, target, username) {
  const from = item.from || (item.direction === 'send' ? username : item.peer);
  const to = item.to !== undefined ? item.to : (item.direction === 'send' ? item.peer : '*');

  if (target === '*') {
    return to === '*';
  }
  return (from === username && to === target) || (from === target && to !== '*');
}

function getTransferUiMeta(t) {
  const progress = Math.max(0, Math.min(100, Number(t.progress ?? 0)));
  const isActive = t.status === 'sending' || t.status === 'receiving' || t.status === 'preparing';
  const isDone = t.status === 'done' || t.status === 'sent to server';
  const isFail = t.status === 'failed' || t.status === 'timed out' || t.status === 'hash mismatch';

  let statusLabel = t.status || '';
  if (t.status === 'preparing') statusLabel = 'Đang chuẩn bị gửi...';
  else if (t.status === 'sending') statusLabel = 'Đang gửi qua UDP...';
  else if (t.status === 'sent to server') statusLabel = '✓ Đã gửi xong';
  else if (t.status === 'receiving') statusLabel = 'Đang nhận qua UDP...';
  else if (t.status === 'done') statusLabel = '✓ Đã nhận xong';
  else if (t.status === 'timed out') statusLabel = '✕ Hết thời gian chờ';
  else if (t.status === 'hash mismatch') statusLabel = '✕ Lỗi kiểm tra SHA-256';
  else if (t.status === 'failed') statusLabel = '✕ Truyền tệp thất bại';

  let fillClass = 'file-progress-fill';
  let statusClass = 'file-status-text';
  if (isActive) fillClass += ' active';
  if (isDone) {
    fillClass += ' done';
    statusClass += ' done';
  }
  if (isFail) {
    fillClass += ' failed';
    statusClass += ' failed';
  }

  let extraInfo = '';
  if (t.path) {
    extraInfo = `Lưu tại: ${t.path}`;
  } else if (t.error) {
    extraInfo = t.error;
  }

  return { progress, isActive, isDone, isFail, statusLabel, fillClass, statusClass, extraInfo };
}

function renderTransferBubble(t, username) {
  const id = t.file_id || t.localId || '';
  const isMe = t.direction === 'send';
  const isDirect = (t.to !== undefined ? t.to : t.peer) !== '*';
  const toTarget = t.to !== undefined ? t.to : t.peer;
  const senderName = t.from || t.peer || '?';
  const recipientText = isDirect ? `tới @${escapeHtml(isMe ? toTarget : username)}` : 'phòng chung';
  const ui = getTransferUiMeta(t);
  const sizeText = typeof t.size === 'number' ? formatBytes(t.size) : '';

  return `
    <div class="message-group ${isMe ? 'outgoing' : 'incoming'}" data-transfer-id="${escapeHtml(id)}">
      <div class="message-author">
        ${isMe ? 'Bạn' : escapeHtml(senderName)}
        ${isDirect ? `<span class="dm-badge">${recipientText}</span>` : ''}
      </div>
      <div class="file-bubble">
        <div class="file-bubble-top">
          <div class="file-bubble-icon">
            <svg width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
              <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path>
              <polyline points="13 2 13 9 20 9"></polyline>
            </svg>
          </div>
          <div class="file-bubble-info">
            <div class="file-bubble-name" title="${escapeHtml(t.name)}">${escapeHtml(t.name)}</div>
            <div class="file-bubble-meta">
              <span>${isMe ? 'Tệp gửi đi' : 'Tệp nhận về'}</span>
              ${sizeText ? `<span>• ${escapeHtml(sizeText)}</span>` : ''}
            </div>
          </div>
        </div>
        <div class="file-progress-track">
          <div class="${ui.fillClass}" style="width: ${ui.progress}%"></div>
        </div>
        <div class="file-progress-footer">
          <span class="${ui.statusClass}">${escapeHtml(ui.statusLabel)}</span>
          <span class="file-progress-percent">${ui.progress}%</span>
        </div>
        <div class="file-saved-path" style="${ui.extraInfo ? '' : 'display:none;'}">${escapeHtml(ui.extraInfo)}</div>
      </div>
      <div class="message-time">${escapeHtml(t.time || '')}</div>
    </div>
  `;
}

function updateTransferCardInPlace(t) {
  const id = t.file_id || t.localId;
  if (!id || !msgContainer) return false;
  const card = msgContainer.querySelector(`[data-transfer-id="${CSS.escape(id)}"]`);
  if (!card) return false;

  const ui = getTransferUiMeta(t);
  const fill = card.querySelector('.file-progress-fill');
  const percent = card.querySelector('.file-progress-percent');
  const statusElem = card.querySelector('.file-status-text');
  const pathElem = card.querySelector('.file-saved-path');

  if (fill) {
    fill.className = ui.fillClass;
    fill.style.width = `${ui.progress}%`;
  }
  if (percent) {
    percent.textContent = `${ui.progress}%`;
  }
  if (statusElem) {
    statusElem.className = ui.statusClass;
    statusElem.textContent = ui.statusLabel;
  }
  if (pathElem) {
    if (ui.extraInfo) {
      pathElem.style.display = '';
      pathElem.textContent = ui.extraInfo;
    } else {
      pathElem.style.display = 'none';
      pathElem.textContent = '';
    }
  }
  return true;
}

function formatRoomMeta(msgCount, fileCount, defaultLabel) {
  const parts = [];
  if (msgCount > 0) parts.push(`${msgCount} tin`);
  if (fileCount > 0) parts.push(`${fileCount} tệp`);
  return parts.length > 0 ? parts.join(' · ') : defaultLabel;
}

function renderState(s) {
  myUsername = s.username;

  // Header user status
  const userDisplay = document.getElementById('user-display');
  const userAvatar = document.getElementById('user-avatar');
  if (userDisplay) userDisplay.textContent = s.username;
  if (userAvatar) userAvatar.textContent = (s.username || '?').charAt(0).toUpperCase();

  const isConnected = s.connection === 'connected';
  const connDot = document.getElementById('conn-dot');
  const connText = document.getElementById('conn-text');
  if (connDot) connDot.className = 'status-dot' + (isConnected ? '' : ' disconnected');
  if (connText) connText.textContent = isConnected ? 'Online (UDP Safe)' : 'Mất kết nối';

  const allMessages = s.messages || [];
  const allTransfers = [...(s.transfers || []), ...pendingUploads];

  // Active Users list
  const otherUsers = (s.users || []).filter(x => x !== s.username);
  const userCount = document.getElementById('user-count');
  if (userCount) userCount.textContent = (s.users ? s.users.length : 0) + ' online';

  const broadcastMsgs = allMessages.filter(m => belongsToRoom(m, '*', s.username)).length;
  const broadcastFiles = allTransfers.filter(t => belongsToRoom(t, '*', s.username));
  const broadcastActive = broadcastFiles.find(t =>
    t.status === 'sending' || t.status === 'receiving' || t.status === 'preparing'
  );

  let userHtml = `
    <div class="user-item broadcast ${currentTarget === '*' ? 'active' : ''}" onclick="selectTarget('*')">
      <div class="user-avatar broadcast-avatar">
        <svg width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon><path d="M19.07 4.93a10 10 0 0 1 0 14.14M15.54 8.46a5 5 0 0 1 0 7.07"></path></svg>
      </div>
      <div class="user-info">
        <div class="user-name">Mọi người (Everyone)</div>
        <div class="user-meta">Phòng chat chung (${escapeHtml(formatRoomMeta(broadcastMsgs, broadcastFiles.length, '0 tin'))})</div>
      </div>
      ${broadcastActive ? `<span class="transfer-pill">${broadcastActive.progress ?? 0}%</span>` : ''}
    </div>
  `;

  otherUsers.forEach(u => {
    const isSelected = currentTarget === u;
    const directMsgs = allMessages.filter(m => belongsToRoom(m, u, s.username)).length;
    const directFiles = allTransfers.filter(t => belongsToRoom(t, u, s.username));
    const activeTransfer = directFiles.find(t =>
      t.status === 'sending' || t.status === 'receiving' || t.status === 'preparing'
    );

    userHtml += `
      <div class="user-item ${isSelected ? 'active' : ''}" onclick="selectTarget('${escapeHtml(u)}')">
        <div class="user-avatar">
          ${escapeHtml(u.charAt(0).toUpperCase())}
          <div class="online-badge"></div>
        </div>
        <div class="user-info">
          <div class="user-name">${escapeHtml(u)}</div>
          <div class="user-meta">${escapeHtml(formatRoomMeta(directMsgs, directFiles.length, 'Chat riêng'))}</div>
        </div>
        ${activeTransfer ? `<span class="transfer-pill">${activeTransfer.progress ?? 0}%</span>` : ''}
      </div>
    `;
  });

  const userListElem = document.getElementById('user-list');
  if (userListElem) userListElem.innerHTML = userHtml;

  // Sync hidden target dropdown
  const targetSelect = document.getElementById('target');
  if (targetSelect) {
    targetSelect.innerHTML = '<option value="*">Everyone</option>' +
      otherUsers.map(x => `<option value="${escapeHtml(x)}">${escapeHtml(x)}</option>`).join('');
    targetSelect.value = currentTarget;
  }

  // Filter messages & file transfers belonging ONLY to the selected chat room
  const roomMessages = allMessages
    .filter(m => belongsToRoom(m, currentTarget, s.username))
    .map((m, idx) => ({
      kind: 'message',
      key: `msg:${idx}:${m.from}:${m.to}:${m.time}:${m.text}`,
      sortTs: typeof m.timestamp === 'number' ? m.timestamp : idx,
      order: idx,
      data: m,
    }));

  const roomTransfers = allTransfers
    .filter(t => belongsToRoom(t, currentTarget, s.username))
    .map((t, idx) => ({
      kind: 'transfer',
      key: `file:${t.file_id || t.localId || idx}`,
      sortTs: typeof t.timestamp === 'number' ? t.timestamp : (1e12 + idx),
      order: idx,
      data: t,
    }));

  const timeline = [...roomMessages, ...roomTransfers].sort((a, b) => {
    if (a.sortTs !== b.sortTs) return a.sortTs - b.sortTs;
    return a.order - b.order;
  });

  const signature = `${currentTarget}|${timeline.map(item => item.key).join(',')}`;

  if (msgContainer) {
    if (signature !== lastTimelineSignature) {
      const timelineHtml = timeline.map(item => {
        if (item.kind === 'transfer') {
          return renderTransferBubble(item.data, s.username);
        }
        const m = item.data;
        const isMe = m.from === s.username;
        const isDirect = m.to !== '*';
        const recipientText = isDirect ? `tới @${escapeHtml(m.to)}` : 'phòng chung';

        return `
          <div class="message-group ${isMe ? 'outgoing' : 'incoming'}">
            <div class="message-author">
              ${isMe ? 'Bạn' : escapeHtml(m.from)}
              ${isDirect ? `<span class="dm-badge">${recipientText}</span>` : ''}
            </div>
            <div class="message-bubble">
              ${escapeHtml(m.text)}
            </div>
            <div class="message-time">${escapeHtml(m.time)}</div>
          </div>
        `;
      }).join('');

      const emptyNotice = currentTarget === '*'
        ? 'Chưa có tin nhắn hoặc tệp nào trong phòng chung. Hãy gửi lời chào hoặc kéo thả tệp vào đây!'
        : `Chưa có tin nhắn hoặc tệp riêng với ${escapeHtml(currentTarget)}. Hãy bắt đầu cuộc trò chuyện bên dưới!`;

      msgContainer.innerHTML = timelineHtml || `
        <div style="text-align:center; padding: 50px 20px; color: var(--text-muted); font-size: 0.9rem;">
          ${emptyNotice}
        </div>
      `;

      lastTimelineSignature = signature;
      if (!userScrolledUp) {
        msgContainer.scrollTop = msgContainer.scrollHeight;
      }
      updateScrollBottomButton();
    } else {
      // Update progress bars in-place so CSS width transitions stay smooth
      roomTransfers.forEach(item => {
        updateTransferCardInPlace(item.data);
      });
    }
  }
}

async function refresh() {
  if (pollTimer) {
    clearTimeout(pollTimer);
    pollTimer = null;
  }

  let s;
  try {
    s = await api('/api/state');
  } catch (error) {
    const dot = document.getElementById('conn-dot');
    const text = document.getElementById('conn-text');
    if (dot) dot.className = 'status-dot disconnected';
    if (text) text.textContent = 'Mất kết nối';
    pollTimer = setTimeout(refresh, 1000);
    return;
  }

  lastState = s;
  renderState(s);

  const hasActiveTransfer = (s.transfers || []).some(
    t => t.status === 'sending' || t.status === 'receiving'
  ) || pendingUploads.length > 0;

  pollTimer = setTimeout(refresh, hasActiveTransfer ? 250 : 700);
}

async function sendMessage() {
  const input = document.getElementById('text');
  const text = input ? input.value.trim() : '';
  if (!text) {
    if (pendingFile) {
      await sendFile();
    }
    return;
  }
  try {
    await api('/api/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ to: currentTarget, text: text })
    });
    input.value = '';
    userScrolledUp = false;
    await refresh();
    if (msgContainer) msgContainer.scrollTop = msgContainer.scrollHeight;
  } catch (error) {
    showNotice(error.message);
  }
}

async function sendFile() {
  const input = document.getElementById('file');
  const f = pendingFile || (input && input.files && input.files[0]);
  if (!f) {
    showNotice('Vui lòng chọn một tệp trước khi gửi.');
    return;
  }
  if (f.size > 50 * 1024 * 1024) {
    showNotice('Tệp vượt quá giới hạn 50 MiB.');
    return;
  }

  const targetRoom = currentTarget;
  const localId = 'local-' + Date.now() + '-' + Math.random().toString(36).slice(2, 7);
  const now = new Date();
  const timeStr = now.toTimeString().slice(0, 8);

  pendingUploads.push({
    localId,
    direction: 'send',
    name: f.name,
    size: f.size,
    from: myUsername,
    to: targetRoom,
    peer: targetRoom,
    status: 'preparing',
    progress: 0,
    time: timeStr,
    timestamp: Date.now() / 1000,
  });

  clearSelectedFile();
  userScrolledUp = false;
  if (lastState) {
    renderState(lastState);
  }

  try {
    const bytes = new Uint8Array(await f.arrayBuffer());
    let binary = '';
    for (let i = 0; i < bytes.length; i += 32768) {
      binary += String.fromCharCode(...bytes.subarray(i, i + 32768));
    }

    await api('/api/file', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        to: targetRoom,
        name: f.name,
        data: btoa(binary)
      })
    });

    pendingUploads = pendingUploads.filter(item => item.localId !== localId);
    await refresh();
  } catch (error) {
    pendingUploads = pendingUploads.filter(item => item.localId !== localId);
    if (lastState) {
      renderState(lastState);
    }
    showNotice(error.message);
  }
}

refresh();
