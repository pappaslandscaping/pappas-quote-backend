# Estimate Description Variants

These variants are designed for Copilot service descriptions in the on-screen estimate view. They use the same visual language as the email system while staying reasonably compact.

## 1. Soft Card

**Why it works well in Copilot's on-screen estimate view:** It feels closest to the email system by using the same soft background, rounded corners, subtle border, and restrained typography without becoming oversized.

```html
<div style="font-family:Helvetica, Arial, sans-serif;color:#2e403d;background:#f7f9f5;border:1px solid #d7dfd1;border-radius:12px;padding:14px 16px;">
  <div style="font-size:11px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#6e7f6d;margin:0 0 8px 0;">What's Included</div>
  <div style="font-size:14px;line-height:1.6;color:#2e403d;margin:0 0 12px 0;">
    Bi-weekly mowing service designed to keep the lawn healthy, neat, and well maintained throughout the season.
  </div>
  <div style="border-top:1px solid #d7dfd1;padding-top:10px;">
    <div style="margin:0 0 8px 0;font-size:13px;line-height:1.6;color:#2e403d;">
      <span style="font-weight:700;color:#2e403d;">Mowing:</span>
      Lawn cut at the proper seasonal height for healthy growth and a clean appearance.
    </div>
    <div style="margin:0 0 8px 0;font-size:13px;line-height:1.6;color:#2e403d;">
      <span style="font-weight:700;color:#2e403d;">Trimming &amp; Cleanup:</span>
      Around trees, flower beds, and pathways, with clippings removed and hard surfaces blown clean.
    </div>
    <div style="margin:0;font-size:13px;line-height:1.6;color:#6e7f6d;">
      <span style="font-weight:700;color:#6e7f6d;">Optional:</span>
      Concrete edging available as an add-on.
    </div>
  </div>
</div>
```

## 2. Editorial

**Why it works well in Copilot's on-screen estimate view:** It looks polished through spacing, hierarchy, and a quiet accent rule rather than a full container, so it reads lighter inside a line item.

```html
<div style="font-family:Helvetica, Arial, sans-serif;color:#2e403d;">
  <div style="width:42px;height:4px;background:#c9dd80;border-radius:999px;margin:0 0 10px 0;"></div>
  <div style="font-size:11px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#6e7f6d;margin:0 0 6px 0;">What's Included</div>
  <div style="font-size:14px;line-height:1.7;color:#2e403d;margin:0 0 12px 0;">
    Bi-weekly mowing service designed to keep the lawn healthy, neat, and well maintained throughout the season.
  </div>
  <div style="border-left:2px solid #d7dfd1;padding-left:12px;">
    <div style="margin:0 0 9px 0;font-size:13px;line-height:1.65;color:#2e403d;">
      <span style="display:block;font-weight:700;color:#2e403d;">Mowing</span>
      Lawn cut at the proper seasonal height for healthy growth and a clean appearance.
    </div>
    <div style="margin:0 0 9px 0;font-size:13px;line-height:1.65;color:#2e403d;">
      <span style="display:block;font-weight:700;color:#2e403d;">Trimming &amp; Cleanup</span>
      Around trees, flower beds, and pathways, with clippings removed and hard surfaces blown clean.
    </div>
    <div style="margin:0;font-size:13px;line-height:1.65;color:#6e7f6d;">
      <span style="display:block;font-weight:700;color:#6e7f6d;">Optional</span>
      Concrete edging available as an add-on.
    </div>
  </div>
</div>
```

## 3. Compact Premium

**Why it works well in Copilot's on-screen estimate view:** It delivers the best polish-to-space ratio by using a slim accent edge, tight spacing, and small definition rows without the height of a full card.

```html
<div style="font-family:Helvetica, Arial, sans-serif;color:#2e403d;border-left:4px solid #c9dd80;padding:10px 0 10px 12px;">
  <div style="font-size:11px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#6e7f6d;margin:0 0 6px 0;">What's Included</div>
  <div style="font-size:13px;line-height:1.6;color:#2e403d;margin:0 0 10px 0;">
    Bi-weekly mowing service designed to keep the lawn healthy, neat, and well maintained throughout the season.
  </div>
  <div style="font-size:12.5px;line-height:1.6;color:#2e403d;margin:0;padding-top:2px;">
    <div style="margin:0 0 6px 0;"><span style="font-weight:700;color:#2e403d;">Mowing:</span> Lawn cut at the proper seasonal height for healthy growth and a clean appearance.</div>
    <div style="margin:0 0 6px 0;"><span style="font-weight:700;color:#2e403d;">Trimming &amp; Cleanup:</span> Around trees, flower beds, and pathways, with clippings removed and hard surfaces blown clean.</div>
    <div style="margin:0;color:#6e7f6d;"><span style="font-weight:700;color:#6e7f6d;">Optional:</span> Concrete edging available as an add-on.</div>
  </div>
</div>
```

## 4. Split Panel

**Why it works well in Copilot's on-screen estimate view:** It creates stronger hierarchy by separating the summary from the included items, but still stays compact enough for a line-item description.

```html
<div style="font-family:Helvetica, Arial, sans-serif;color:#2e403d;border:1px solid #d7dfd1;border-radius:12px;overflow:hidden;">
  <div style="background:#f7f9f5;padding:12px 14px 10px 14px;border-bottom:1px solid #d7dfd1;">
    <div style="font-size:11px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#6e7f6d;margin:0 0 6px 0;">What's Included</div>
    <div style="font-size:14px;line-height:1.6;color:#2e403d;margin:0;">
      Bi-weekly mowing service designed to keep the lawn healthy, neat, and well maintained throughout the season.
    </div>
  </div>
  <div style="padding:12px 14px;">
    <div style="margin:0 0 8px 0;font-size:13px;line-height:1.6;color:#2e403d;">
      <span style="display:inline-block;min-width:132px;font-weight:700;color:#2e403d;">Mowing</span>
      Lawn cut at the proper seasonal height for healthy growth and a clean appearance.
    </div>
    <div style="margin:0 0 8px 0;font-size:13px;line-height:1.6;color:#2e403d;">
      <span style="display:inline-block;min-width:132px;font-weight:700;color:#2e403d;">Trimming &amp; Cleanup</span>
      Around trees, flower beds, and pathways, with clippings removed and hard surfaces blown clean.
    </div>
    <div style="margin:0;font-size:13px;line-height:1.6;color:#6e7f6d;">
      <span style="display:inline-block;min-width:132px;font-weight:700;color:#6e7f6d;">Optional</span>
      Concrete edging available as an add-on.
    </div>
  </div>
</div>
```

## 5. Accent Rail

**Why it works well in Copilot's on-screen estimate view:** It feels premium without being bulky by using a narrow accent rail, soft inset background, and small structured content blocks.

```html
<div style="font-family:Helvetica, Arial, sans-serif;color:#2e403d;background:linear-gradient(to right,#c9dd80 0,#c9dd80 6px,#f7f9f5 6px,#f7f9f5 100%);border-radius:12px;padding:12px 14px 12px 18px;">
  <div style="font-size:11px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#6e7f6d;margin:0 0 6px 0;">What's Included</div>
  <div style="font-size:13.5px;line-height:1.65;color:#2e403d;margin:0 0 10px 0;">
    Bi-weekly mowing service designed to keep the lawn healthy, neat, and well maintained throughout the season.
  </div>
  <div style="margin:0 0 8px 0;padding-top:8px;border-top:1px solid #d7dfd1;font-size:13px;line-height:1.6;color:#2e403d;">
    <span style="font-weight:700;color:#2e403d;">Mowing:</span> Lawn cut at the proper seasonal height for healthy growth and a clean appearance.
  </div>
  <div style="margin:0 0 8px 0;font-size:13px;line-height:1.6;color:#2e403d;">
    <span style="font-weight:700;color:#2e403d;">Trimming &amp; Cleanup:</span> Around trees, flower beds, and pathways, with clippings removed and hard surfaces blown clean.
  </div>
  <div style="margin:0;font-size:13px;line-height:1.6;color:#6e7f6d;">
    <span style="font-weight:700;color:#6e7f6d;">Optional:</span> Concrete edging available as an add-on.
  </div>
</div>
```

## 6. Refined List

**Why it works well in Copilot's on-screen estimate view:** It stays very efficient vertically while still feeling designed, using compact rows, light rules, and stronger typography instead of a larger container.

```html
<div style="font-family:Helvetica, Arial, sans-serif;color:#2e403d;">
  <div style="font-size:11px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#6e7f6d;margin:0 0 7px 0;">What's Included</div>
  <div style="font-size:13.5px;line-height:1.65;color:#2e403d;margin:0 0 10px 0;">
    Bi-weekly mowing service designed to keep the lawn healthy, neat, and well maintained throughout the season.
  </div>
  <div style="border-top:1px solid #d7dfd1;">
    <div style="padding:8px 0;border-bottom:1px solid #d7dfd1;font-size:12.5px;line-height:1.55;color:#2e403d;">
      <span style="font-weight:700;color:#2e403d;">Mowing</span><br />
      Lawn cut at the proper seasonal height for healthy growth and a clean appearance.
    </div>
    <div style="padding:8px 0;border-bottom:1px solid #d7dfd1;font-size:12.5px;line-height:1.55;color:#2e403d;">
      <span style="font-weight:700;color:#2e403d;">Trimming &amp; Cleanup</span><br />
      Around trees, flower beds, and pathways, with clippings removed and hard surfaces blown clean.
    </div>
    <div style="padding:8px 0 0 0;font-size:12.5px;line-height:1.55;color:#6e7f6d;">
      <span style="font-weight:700;color:#6e7f6d;">Optional</span><br />
      Concrete edging available as an add-on.
    </div>
  </div>
</div>
```

## 7. Studio Card

**Why it works well in Copilot's on-screen estimate view:** This is one of the strongest production options because it feels finished and premium, but the content still scans quickly in a narrow service-description area.

```html
<div style="font-family:Helvetica, Arial, sans-serif;color:#2e403d;background:#f7f9f5;border:1px solid #d7dfd1;border-radius:14px;padding:14px 16px 15px 16px;">
  <div style="display:inline-block;font-size:11px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#6e7f6d;padding:4px 8px;background:#ffffff;border:1px solid #d7dfd1;border-radius:999px;margin:0 0 10px 0;">
    What's Included
  </div>
  <div style="font-size:14px;line-height:1.65;color:#2e403d;margin:0 0 12px 0;">
    Bi-weekly mowing service designed to keep the lawn healthy, neat, and well maintained throughout the season.
  </div>
  <div style="border-top:1px solid #d7dfd1;padding-top:10px;">
    <div style="margin:0 0 8px 0;font-size:13px;line-height:1.6;color:#2e403d;">
      <span style="font-weight:700;color:#2e403d;">Mowing:</span>
      Lawn cut at the proper seasonal height for healthy growth and a clean appearance.
    </div>
    <div style="margin:0 0 8px 0;font-size:13px;line-height:1.6;color:#2e403d;">
      <span style="font-weight:700;color:#2e403d;">Trimming &amp; Cleanup:</span>
      Around trees, flower beds, and pathways, with clippings removed and hard surfaces blown clean.
    </div>
    <div style="margin:0;font-size:13px;line-height:1.6;color:#6e7f6d;">
      <span style="font-weight:700;color:#6e7f6d;">Optional:</span>
      Concrete edging available as an add-on.
    </div>
  </div>
</div>
```

## 8. Service Ledger

**Why it works well in Copilot's on-screen estimate view:** This is one of the best practical options because it uses a compact, highly scannable left-label structure that still feels designed instead of utilitarian.

```html
<div style="font-family:Helvetica, Arial, sans-serif;color:#2e403d;">
  <div style="font-size:11px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#6e7f6d;margin:0 0 7px 0;">What's Included</div>
  <div style="font-size:13.5px;line-height:1.65;color:#2e403d;margin:0 0 12px 0;">
    Bi-weekly mowing service designed to keep the lawn healthy, neat, and well maintained throughout the season.
  </div>
  <div style="background:#f7f9f5;border:1px solid #d7dfd1;border-radius:10px;padding:10px 12px;">
    <div style="display:flex;border-bottom:1px solid #d7dfd1;padding:8px 0;">
      <div style="width:122px;min-width:122px;font-size:12.5px;font-weight:700;line-height:1.55;color:#2e403d;">Mowing</div>
      <div style="font-size:12.5px;line-height:1.55;color:#2e403d;">Lawn cut at the proper seasonal height for healthy growth and a clean appearance.</div>
    </div>
    <div style="display:flex;border-bottom:1px solid #d7dfd1;padding:8px 0;">
      <div style="width:122px;min-width:122px;font-size:12.5px;font-weight:700;line-height:1.55;color:#2e403d;">Trimming &amp; Cleanup</div>
      <div style="font-size:12.5px;line-height:1.55;color:#2e403d;">Around trees, flower beds, and pathways, with clippings removed and hard surfaces blown clean.</div>
    </div>
    <div style="display:flex;padding:8px 0 0 0;">
      <div style="width:122px;min-width:122px;font-size:12.5px;font-weight:700;line-height:1.55;color:#6e7f6d;">Optional</div>
      <div style="font-size:12.5px;line-height:1.55;color:#6e7f6d;">Concrete edging available as an add-on.</div>
    </div>
  </div>
</div>
```

## 9. Quiet Premium

**Why it works well in Copilot's on-screen estimate view:** This is one of the best high-end options because it feels intentional and polished through spacing and accent balance, without relying on a heavy container.

```html
<div style="font-family:Helvetica, Arial, sans-serif;color:#2e403d;padding:2px 0;">
  <div style="display:flex;align-items:center;margin:0 0 8px 0;">
    <div style="width:28px;height:2px;background:#c9dd80;margin-right:8px;"></div>
    <div style="font-size:11px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#6e7f6d;">What's Included</div>
  </div>
  <div style="font-size:14px;line-height:1.7;color:#2e403d;margin:0 0 12px 0;">
    Bi-weekly mowing service designed to keep the lawn healthy, neat, and well maintained throughout the season.
  </div>
  <div style="padding-left:12px;border-left:3px solid #d7dfd1;">
    <div style="margin:0 0 9px 0;font-size:13px;line-height:1.6;color:#2e403d;">
      <span style="font-weight:700;color:#2e403d;">Mowing:</span>
      Lawn cut at the proper seasonal height for healthy growth and a clean appearance.
    </div>
    <div style="margin:0 0 9px 0;font-size:13px;line-height:1.6;color:#2e403d;">
      <span style="font-weight:700;color:#2e403d;">Trimming &amp; Cleanup:</span>
      Around trees, flower beds, and pathways, with clippings removed and hard surfaces blown clean.
    </div>
    <div style="margin:0;font-size:13px;line-height:1.6;color:#6e7f6d;">
      <span style="font-weight:700;color:#6e7f6d;">Optional:</span>
      Concrete edging available as an add-on.
    </div>
  </div>
</div>
```
