const fs = require('fs');
const path = require('path');

const serverCode = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
const briefingStart = serverCode.indexOf('async function assembleMorningBriefing()');
const briefingEnd = serverCode.indexOf("app.post('/api/morning-briefing'", briefingStart);
const briefingCode = serverCode.slice(briefingStart, briefingEnd);

describe('morning briefing live HomeWorks jobs', () => {
  test('refreshes the official HomeWorks schedule before reading the mirror', () => {
    const refreshIndex = briefingCode.indexOf('fetchLiveCopilotScheduleDate({');
    const mirrorIndex = briefingCode.indexOf('FROM copilot_live_jobs');
    expect(refreshIndex).toBeGreaterThan(-1);
    expect(mirrorIndex).toBeGreaterThan(refreshIndex);
    expect(briefingCode).toContain("stats.scheduleSource = 'official_homeworks_graphql'");
  });

  test('never describes a failed live refresh as an empty schedule', () => {
    expect(briefingCode).toContain('Couldn’t load today’s jobs from HomeWorks');
    expect(briefingCode).toContain('Using the last saved HomeWorks schedule');
  });
});
