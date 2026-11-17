const {
  buildCommunicationCallNote,
  parseHomeWorksCallNotesHtml,
  saveHomeWorksCallNote,
} = require('../services/homeworks/call-notes');

describe('HomeWorks Call Notes integration', () => {
  test('parses recent call notes from the HomeWorks customer timeline', () => {
    const notes = parseHomeWorksCallNotesHtml(`
      <ul id="custom_timeline_communicat_tab">
        <li>
          <div class="text-primary"><a href="/customers/details/42"><strong><u>Sep 21, 2026</u></strong></a> by <a href="/resources/employees/edit/9">Theresa Pappas</a></div>
          <div class="text-dark">Twilio Connect · Text received<br>Message:<br>Hello there</div>
        </li>
      </ul>
    `);
    expect(notes).toEqual([expect.objectContaining({
      date: 'Sep 21, 2026',
      author: 'Theresa Pappas',
      body: 'Twilio Connect · Text received\nMessage:\nHello there',
    })]);
  });

  test('formats an inbound text as a readable communication journal entry', () => {
    const note = buildCommunicationCallNote({
      kind: 'text',
      direction: 'inbound',
      occurredAt: '2026-09-21T13:33:00.000Z',
      from: '+12165012439',
      to: '+12165551212',
      body: 'Please wait until mid April.',
      status: 'received',
      sourceId: 'SM123',
    });

    expect(note).toContain('Twilio Connect · Text received');
    expect(note).toContain('Sep 21, 2026');
    expect(note).toContain('Please wait until mid April.');
    expect(note).toContain('Twilio ID: SM123');
  });

  test('formats a voicemail with its duration, transcription, and recording link', () => {
    const note = buildCommunicationCallNote({
      kind: 'voicemail',
      direction: 'inbound',
      occurredAt: '2026-09-21T14:10:00.000Z',
      from: '+12165012439',
      to: '+12165551212',
      duration: 74,
      transcription: 'Please call me about the fall cleanup.',
      recordingUrl: 'https://example.test/api/recordings/RE123',
      status: 'voicemail',
      sourceId: 'CA123',
    });

    expect(note).toContain('Twilio Connect · Voicemail received');
    expect(note).toContain('Duration: 1m 14s');
    expect(note).toContain('Please call me about the fall cleanup.');
    expect(note).toContain('Recording: https://example.test/api/recordings/RE123');
    expect(note).toContain('Twilio ID: CA123');
  });

  test('saves an office note with an explicit local-only result',async()=>{
 const records=require('../services/homeworks/records');const verify=jest.spyOn(records,'uniqueCustomer').mockResolvedValue({id:42});
 try{const pool={query:jest.fn().mockResolvedValue({rows:[{id:1}]})},fetchImpl=jest.fn();
 const result=await saveHomeWorksCallNote({pool,customerId:42,note:'Please call Jane',fetchImpl});
 expect(result).toMatchObject({success:true,status:'saved_local',destination:'office_notes',homeworksSynced:false});expect(fetchImpl).not.toHaveBeenCalled();
 expect(pool.query).toHaveBeenCalledWith(expect.stringContaining('INSERT INTO yarddesk_customer_notes'),[42,'Please call Jane']);
 }finally{verify.mockRestore();}
});

  test('does not save an office note against an unverified Homeworks customer',async()=>{
 const records=require('../services/homeworks/records');const verify=jest.spyOn(records,'uniqueCustomer').mockRejectedValue(new Error('No verified customer'));
 try{const pool={query:jest.fn()};await expect(saveHomeWorksCallNote({pool,customerId:42,note:'Test note'})).rejects.toThrow('No verified customer');expect(pool.query).not.toHaveBeenCalled();}finally{verify.mockRestore();}
});
});
