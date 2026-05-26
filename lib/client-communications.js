const CLIENT_COMMUNICATIONS_DISABLED_MESSAGE =
  'Backend client communications are disabled. Send customer messages through CopilotCRM. The only allowed backend client email is the contract generated from an accepted CopilotCRM estimate.';

function clientCommunicationsDisabledResponse(res) {
  return res.status(403).json({
    success: false,
    error: CLIENT_COMMUNICATIONS_DISABLED_MESSAGE,
    clientCommunicationsDisabled: true,
  });
}

module.exports = {
  CLIENT_COMMUNICATIONS_DISABLED_MESSAGE,
  clientCommunicationsDisabledResponse,
};
