const turns = [
  {
    prompt: 'Find the source of the timeout parsing bug.',
    response:
      'I traced the failure to the timeout handling path and reproduced the behavior.',
  },
  {
    prompt: 'Implement the fix and verify it.',
    response:
      'The implementation and regression coverage are in place, and the focused tests pass.',
  },
  {
    prompt: 'Give it a final review.',
    response:
      'The diff, formatting, and lint checks look good. The change is ready for review.',
  },
];

const elements = {
  form: document.querySelector('#prompt-form'),
  prompt: document.querySelector('#prompt'),
  send: document.querySelector('#send-turn'),
  transcript: document.querySelector('#transcript'),
  status: document.querySelector('#agent-status'),
  note: document.querySelector('#composer-note'),
  workflowId: document.querySelector('#workflow-id'),
  syncSummary: document.querySelector('#sync-summary'),
  syncCursor: document.querySelector('#sync-cursor'),
  localCount: document.querySelector('#local-count'),
  upstreamCount: document.querySelector('#upstream-count'),
  localUi: document.querySelector('#local-ui'),
  upstreamUi: document.querySelector('#upstream-ui'),
};

let state = null;
let sending = false;
let visibleToolCount = 0;

function message(role, text) {
  const article = document.createElement('article');
  article.className = `message ${role}`;
  const label = document.createElement('span');
  label.className = 'message-label';
  label.textContent = role === 'user' ? 'You' : 'Agent';
  const paragraph = document.createElement('p');
  paragraph.textContent = text;
  article.append(label, paragraph);
  return article;
}

function renderTool(tool, displayIndex) {
  const card = document.createElement('div');
  card.className = 'tool-call';
  const heading = document.createElement('div');
  heading.className = 'tool-heading';
  const index = document.createElement('span');
  index.className = 'tool-index';
  index.textContent = String(displayIndex + 1).padStart(2, '0');
  const title = document.createElement('div');
  title.className = 'tool-title';
  title.textContent = tool.summary ?? 'Missing Activity summary';
  const activityState = document.createElement('span');
  activityState.className = `activity-state ${tool.status}`;
  const dot = document.createElement('i');
  dot.setAttribute('aria-hidden', 'true');
  const duration = document.createElement('span');
  duration.className = 'tool-meta';
  duration.textContent = tool.status === 'running' ? 'Activity running' : 'Activity completed';
  activityState.append(dot, duration);
  heading.append(index, title, activityState);
  card.appendChild(heading);
  return card;
}

function renderTranscript(nextState) {
  const fragment = document.createDocumentFragment();
  const visibleTurns = Math.max(
    nextState.completed_turns,
    nextState.active_turn === null ? 0 : nextState.active_turn + 1,
  );
  if (visibleTurns === 0) {
    const empty = document.createElement('div');
    empty.className = 'empty-state';
    empty.textContent = 'Send the first canned prompt to begin the workflow.';
    fragment.appendChild(empty);
  }

  let toolIndex = 0;
  turns.slice(0, visibleTurns).forEach((turn, turnIndex) => {
    const section = document.createElement('section');
    section.className = 'turn';
    section.appendChild(message('user', turn.prompt));
    const tools = nextState.tools.filter((tool) => tool.turn === turnIndex);
    if (tools.length > 0) {
      const stack = document.createElement('div');
      stack.className = 'tool-stack';
      stack.setAttribute('aria-label', `Turn ${turnIndex + 1} tool Activities`);
      tools.forEach((tool) => {
        stack.appendChild(renderTool(tool, toolIndex));
        toolIndex += 1;
      });
      section.appendChild(stack);
    }
    if (turnIndex < nextState.completed_turns) {
      section.appendChild(message('agent', turn.response));
    }
    fragment.appendChild(section);
  });

  if (nextState.phase === 'syncing') {
    const syncing = document.createElement('div');
    syncing.className = 'syncing-message';
    const dot = document.createElement('i');
    dot.setAttribute('aria-hidden', 'true');
    const text = document.createElement('span');
    text.textContent = 'Turn complete · explicitly synchronizing local history upstream';
    syncing.append(dot, text);
    fragment.appendChild(syncing);
  }
  if (nextState.error) {
    const error = document.createElement('div');
    error.className = 'error-banner';
    error.setAttribute('role', 'alert');
    error.textContent = nextState.error;
    fragment.appendChild(error);
  }

  elements.transcript.replaceChildren(fragment);
  if (nextState.tools.length > visibleToolCount) {
    elements.transcript.scrollTop = elements.transcript.scrollHeight;
  }
  visibleToolCount = nextState.tools.length;
}

function render(nextState) {
  state = nextState;
  const localCount = nextState.local_event_count;
  const upstreamCount = nextState.upstream_event_count;
  const lag = Math.max(0, localCount - upstreamCount);
  elements.workflowId.textContent = nextState.workflow_id;
  elements.localCount.textContent = `${localCount} events`;
  elements.upstreamCount.textContent = `${upstreamCount} events`;
  elements.syncCursor.textContent = `Synchronized through event ${nextState.sync_cursor || '—'}`;
  elements.syncSummary.textContent =
    lag === 0 ? 'Histories aligned' : `${lag} event${lag === 1 ? '' : 's'} local-only`;
  elements.syncSummary.classList.toggle('behind', lag > 0);
  if (!elements.localUi.dataset.loaded) {
    elements.localUi.src = nextState.local_ui_path;
    elements.localUi.dataset.loaded = 'true';
  }
  if (!elements.upstreamUi.dataset.loaded) {
    elements.upstreamUi.src = nextState.upstream_ui_path;
    elements.upstreamUi.dataset.loaded = 'true';
  }

  const nextTurn = turns[nextState.completed_turns];
  elements.prompt.value = nextTurn?.prompt ?? 'Canned session complete';
  elements.send.disabled = sending || !nextState.can_submit;
  if (nextState.phase === 'running') {
    elements.status.textContent = `Turn ${(nextState.active_turn ?? 0) + 1} · executing ordinary Activities locally`;
    elements.note.textContent = 'Each tool call is a durable Activity Task.';
  } else if (nextState.phase === 'syncing') {
    elements.status.textContent = 'Waiting for user · explicit sync in progress';
    elements.note.textContent = 'The next prompt unlocks when upstream reaches the local cursor.';
  } else if (nextState.phase === 'complete') {
    elements.status.textContent = 'Three turns complete';
    elements.note.textContent = 'Every completed turn is durable upstream.';
  } else if (nextState.phase === 'error') {
    elements.status.textContent = 'Activity execution stopped';
    elements.note.textContent = 'Inspect the demo process logs.';
  } else {
    elements.status.textContent = `${nextState.completed_turns} of ${turns.length} turns complete · waiting for user`;
    elements.note.textContent = 'Send the next canned prompt.';
  }

  renderTranscript(nextState);
}

async function refresh() {
  try {
    const response = await fetch('/api/state', { cache: 'no-store' });
    if (!response.ok) throw new Error(`State request failed (${response.status})`);
    render(await response.json());
  } catch (error) {
    elements.status.textContent = 'Demo connection lost';
    elements.note.textContent = error.message;
    elements.send.disabled = true;
  }
}

elements.form.addEventListener('submit', async (event) => {
  event.preventDefault();
  if (!state?.can_submit || sending) return;
  sending = true;
  elements.send.disabled = true;
  elements.note.textContent = 'Submitting the turn to the local workflow…';
  try {
    const response = await fetch('/api/turn', { method: 'POST', body: '{}' });
    const body = await response.json();
    if (!response.ok) throw new Error(body.error ?? 'Turn submission failed');
    await refresh();
  } catch (error) {
    elements.note.textContent = error.message;
  } finally {
    sending = false;
  }
});

refresh();
setInterval(refresh, 150);
