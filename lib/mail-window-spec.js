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
    leftIn: 0.88,
    topIn: 2.58,
    widthIn: 4.1,
  }),
});

const RETURN_ENVELOPE9_SPEC = Object.freeze({
  envelope: Object.freeze({
    widthIn: 8.875,
    heightIn: 3.875,
  }),
  topWindow: Object.freeze({
    leftIn: 0.375,
    bottomIn: 2.0,
    widthIn: 3.5,
    heightIn: 1.1875,
  }),
  bottomWindow: Object.freeze({
    leftIn: 0.375,
    bottomIn: 0.375,
    widthIn: 4.0,
    heightIn: 1.0,
  }),
  topAddressZone: Object.freeze({
    leftIn: 0.5,
    bottomIn: 1.98,
    widthIn: 3.24,
    heightIn: 0.95,
  }),
  bottomAddressZone: Object.freeze({
    leftIn: 0.5,
    bottomIn: 0.5,
    widthIn: 3.72,
    heightIn: 0.77,
  }),
});

module.exports = {
  MAIL_WINDOW_SPEC,
  RETURN_ENVELOPE9_SPEC,
};
