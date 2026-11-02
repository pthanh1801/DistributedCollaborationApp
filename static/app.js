let currentTarget = '*';
let lastMessageCount = 0;
let userScrolledUp = false;
let myUsername = '';

const msgContainer = document.getElementById('messages');
if (msgContainer) {
  msgContainer.addEventListener('scroll', () => {
    const isNearBottom = msgContainer.scrollHeight - msgContainer.scrollTop - msgContainer.clientHeight < 60;
    userScrolledUp = !isNearBottom;
  });
}

const textInput = document.getElementById('text');
if (textInput) {
  textInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  });
}

function handleFileSelect(input) {
  const infoBox = document.getElementById('selected-file-box');
  if (input.files && input.files.length > 0) {
    const file = input.files[0];
    document.getElementById('selected-file-name').textContent = file.name;
    document.getElementById('selected-file-size').textContent = (file.size / (1024 * 1024)).toFixed(2) + ' MB';
    infoBox.style.display = 'flex';
  } else {
    infoBox.style.display = 'none';
  }
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

  // Force scroll to bottom when switching rooms
  userScrolledUp = false;
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

async function refresh() {
  let s;
  try {
    s = await api('/api/state');
  } catch (error) {
    const dot = document.getElementById('conn-dot');
    const text = document.getElementById('conn-text');
    if (dot) dot.className = 'status-dot disconnected';
    if (text) text.textContent = 'Mất kết nối';
    return;
  }

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

  // Active Users list
  const otherUsers = (s.users || []).filter(x => x !== s.username);
  const userCount = document.getElementById('user-count');
  if (userCount) userCount.textContent = (s.users ? s.users.length : 0) + ' online';

  const broadcastCount = (s.messages || []).filter(m => m.to === '*').length;

  let userHtml = `
    <div class="user-item broadcast ${currentTarget === '*' ? 'active' : ''}" onclick="selectTarget('*')">
      <div class="user-avatar broadcast-avatar">
        <svg width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon><path d="M19.07 4.93a10 10 0 0 1 0 14.14M15.54 8.46a5 5 0 0 1 0 7.07"></path></svg>
      </div>
      <div class="user-info">
        <div class="user-name">Mọi người (Everyone)</div>
        <div class="user-meta">Phòng chat chung (${broadcastCount} tin)</div>
      </div>
    </div>
  `;

  otherUsers.forEach(u => {
    const isSelected = currentTarget === u;
    const directCount = (s.messages || []).filter(m =>
      (m.from === s.username && m.to === u) || (m.from === u && m.to === s.username)
    ).length;

    userHtml += `
      <div class="user-item ${isSelected ? 'active' : ''}" onclick="selectTarget('${escapeHtml(u)}')">
        <div class="user-avatar">
          ${escapeHtml(u.charAt(0).toUpperCase())}
          <div class="online-badge"></div>
        </div>
        <div class="user-info">
          <div class="user-name">${escapeHtml(u)}</div>
          <div class="user-meta">${directCount > 0 ? directCount + ' tin nhắn' : 'Chat riêng'}</div>
        </div>
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

  // Tách biệt phòng chat:
  // - Nếu chọn "*": Chỉ hiển thị tin nhắn tới phòng chung (m.to === "*")
  // - Nếu chọn một user cụ thể: Chỉ hiển thị tin nhắn giữa mình và user đó
  const filteredMessages = (s.messages || []).filter(m => {
    if (currentTarget === '*') {
      return m.to === '*';
    } else {
      return (m.from === s.username && m.to === currentTarget) ||
             (m.from === currentTarget && m.to === s.username);
    }
  });

  const msgList = filteredMessages.map(m => {
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
    ? 'Chưa có tin nhắn nào trong phòng chung. Hãy gửi lời chào đầu tiên!'
    : `Chưa có cuộc trò chuyện riêng với ${escapeHtml(currentTarget)}. Hãy gửi tin nhắn bên dưới!`;

  if (msgContainer) {
    msgContainer.innerHTML = msgList || `
      <div style="text-align:center; padding: 50px 20px; color: var(--text-muted); font-size: 0.9rem;">
        ${emptyNotice}
      </div>
    `;

    // Auto scroll if user hasn't scrolled up or messages changed
    if (filteredMessages.length !== lastMessageCount) {
      if (!userScrolledUp) {
        msgContainer.scrollTop = msgContainer.scrollHeight;
      }
      lastMessageCount = filteredMessages.length;
    }
  }

  // Transfers
  const transfersElem = document.getElementById('transfers');
  if (transfersElem) {
    transfersElem.innerHTML = (s.transfers || []).map(t => {
      const isSend = t.direction === 'send';
      const isDone = t.status === 'done' || t.status === 'sent to server';
      const isFail = t.status === 'failed' || t.status === 'timed out' || t.status === 'hash mismatch';

      let statusClass = '';
      if (isDone) statusClass = 'done';
      if (isFail) statusClass = 'failed';

      return `
        <div class="transfer-card">
          <div class="transfer-header">
            <div class="transfer-name" title="${escapeHtml(t.name)}">${escapeHtml(t.name)}</div>
            <span class="transfer-type ${t.direction}">${isSend ? 'Gửi đi' : 'Nhận'}</span>
          </div>
          <div class="transfer-peer">${isSend ? 'Đến:' : 'Từ:'} <b>${escapeHtml(t.peer)}</b></div>
          <div class="progress-bar-wrap">
            <div class="progress-bar-inner" style="width: ${t.progress}%"></div>
          </div>
          <div class="transfer-footer">
            <span class="transfer-status ${statusClass}">
              ${isDone ? '✓ ' : ''}${escapeHtml(t.status)}
            </span>
            <span class="transfer-percent">${t.progress}%</span>
          </div>
        </div>
      `;
    }).join('') || `
      <div style="text-align:center; padding: 30px 10px; color: var(--text-muted); font-size: 0.8rem;">
        Chưa có hoạt động truyền file
      </div>
    `;
  }
}

async function sendMessage() {
  const textInput = document.getElementById('text');
  const text = textInput.value.trim();
  if (!text) return;
  try {
    await api('/api/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ to: currentTarget, text: text })
    });
    textInput.value = '';
    refresh();
    setTimeout(() => {
      if (msgContainer) msgContainer.scrollTop = msgContainer.scrollHeight;
    }, 100);
  } catch (error) {
    showNotice(error.message);
  }
}

async function sendFile() {
  const input = document.getElementById('file');
  if (!input.files.length) {
    showNotice('Vui lòng chọn một tệp trước khi gửi.');
    return;
  }
  const f = input.files[0];
  if (f.size > 50 * 1024 * 1024) {
    showNotice('Tệp vượt quá giới hạn 50 MiB.');
    return;
  }

  showNotice(`Đang mã hóa và chuẩn bị truyền "${f.name}" tới [${currentTarget === '*' ? 'Everyone' : currentTarget}]...`, true);

  const bytes = new Uint8Array(await f.arrayBuffer());
  let binary = '';
  for (let i = 0; i < bytes.length; i += 32768) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 32768));
  }

  try {
    await api('/api/file', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        to: currentTarget,
        name: f.name,
        data: btoa(binary)
      })
    });
    showNotice(`Đã đưa tệp "${f.name}" vào hàng đợi gửi UDP!`, true);
    input.value = '';
    const infoBox = document.getElementById('selected-file-box');
    if (infoBox) infoBox.style.display = 'none';
    refresh();
  } catch (error) {
    showNotice(error.message);
  }
}

setInterval(refresh, 700);
refresh();
