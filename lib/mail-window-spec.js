const MAIL_WINDOW_SPEC = Object.freeze({
  envelope: Object.freeze({
    widthIn: 9.5,
    heightIn: 4.125,
  }),
  window: Object.freeze({
    leftIn: 0.875,
    bottomIn: 0.5,
    widthIn: 4.5,
    heightIn: 1.125,
  }),
  insert: Object.freeze({
    widthIn: 8.5,
    heightIn: 11,
  }),
  // Keep the printed recipient block slightly inset from the nominal window
  // so a standard tri-fold still preserves USPS-style clearance if the insert
  // shifts a bit inside the envelope.
  recipientZone: Object.freeze({
    leftIn: 0.84,
    topIn: 2.58,
    widthIn: 4.1,
  }),
});

module.exports = {
  MAIL_WINDOW_SPEC,
};
