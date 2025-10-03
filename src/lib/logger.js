function formatMessage(level, scope, message) {
  const timestamp = new Date().toISOString();
  return `[${timestamp}] [${level.toUpperCase()}]${scope ? ` [${scope}]` : ''} ${message}`;
}

function createLogger(scope = '') {
  return {
    info: (message) => console.log(formatMessage('info', scope, message)),
    warn: (message) => console.warn(formatMessage('warn', scope, message)),
    error: (message) => console.error(formatMessage('error', scope, message)),
    child: (childScope) => createLogger(scope ? `${scope}:${childScope}` : childScope),
  };
}

module.exports = {
  createLogger,
};
