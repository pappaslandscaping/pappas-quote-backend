'use strict';
const records = require('./records');
async function saveCrew(context, input, crewId) {
  const data = await records.call(
    context,
    'YardDeskCrewOptions',
    'query YardDeskCrewOptions{users(where:{isDeleted:false,status:ACTIVE}){id firstName lastName} crews{id name}}',
  );
  const remote = {};
  for (const key of ['name', 'color', 'notes']) if (input[key] !== undefined) remote[key] = input[key];
  if (!crewId && data.crews.some((crew) => crew.name === remote.name))
    throw new Error('A Homeworks crew with this name already exists.');
  if (!crewId && !String(remote.name || '').trim()) throw new Error('Enter a crew name.');
  if (crewId && !data.crews.some((crew) => crew.id === Number(crewId)))
    throw new Error('The Homeworks crew is unavailable.');
  let members = null;
  if (input.members !== undefined || input.user_ids !== undefined) {
    const names = Array.isArray(input.user_ids)
      ? input.user_ids
      : String(input.members || '')
          .split(',')
          .map((name) => name.trim())
          .filter(Boolean);
    members = names.map((value) => {
      const matches = data.users.filter(
        (user) =>
          Number(value) === user.id ||
          `${user.firstName} ${user.lastName}`.toLowerCase() === String(value).toLowerCase(),
      );
      if (matches.length !== 1) throw new Error('Select each crew member by their Homeworks name.');
      return matches[0].id;
    });
    if (new Set(members).size !== members.length) throw new Error('A crew member appears more than once.');
  }
  const operation = crewId ? 'updateCrew' : 'createCrew',
    type = crewId ? 'UpdateCrewInput' : 'CreateCrewInput';
  const result = await records.call(
    context,
    'YardDeskSaveCrew',
    `mutation YardDeskSaveCrew($input:${type}!${crewId ? ', $id:SafeInt!' : ''}){${operation}(${crewId ? 'crewId:$id,' : ''}input:$input){id}}`,
    { input: remote, ...(crewId ? { id: records.id(crewId) } : {}) },
  );
  const savedId = records.id(result[operation]?.id);
  if (members)
    await records.call(
      context,
      'YardDeskCrewMembers',
      'mutation YardDeskCrewMembers($id:SafeInt!,$users:[SafeInt!]!){setCrewMembers(crewId:$id,userIds:$users,reassignOpenEvents:false){id}}',
      { id: savedId, users: members },
    );
  const check = await records.call(
    context,
    'YardDeskVerifyCrew',
    'query YardDeskVerifyCrew($id:SafeInt!){crew(crewId:$id){id name notes color users{id firstName lastName}}}',
    { id: savedId },
  );
  if (
    !check.crew ||
    (remote.name !== undefined && check.crew.name !== remote.name) ||
    (members &&
      (check.crew.users.length !== members.length ||
        check.crew.users.some((user) => !members.includes(user.id))))
  )
    throw new Error('The Homeworks crew edit needs verification. Refresh before retrying.');
  return {
    success: true,
    source: records.SOURCE,
    crew: {
      ...check.crew,
      members: check.crew.users.map((user) => `${user.firstName} ${user.lastName}`).join(', '),
    },
  };
}
async function deleteCrew(context, value) {
  const id = records.id(value);
  const check = await records.call(
    context,
    'YardDeskCrewUsage',
    'query YardDeskCrewUsage($id:SafeInt!){events(where:{isDeleted:false,crewId:{equals:$id},status:{in:[OPEN,WAITLISTED]}},take:1){id}}',
    { id },
  );
  if (check.events?.length)
    throw new Error('Reassign this crew’s open visits in Homeworks before deleting it.');
  await records.call(
    context,
    'YardDeskDeleteCrew',
    'mutation YardDeskDeleteCrew($id:SafeInt!){deleteCrew(crewId:$id)}',
    { id },
  );
  const result = await records.call(
    context,
    'YardDeskVerifyCrewDeleted',
    'query YardDeskVerifyCrewDeleted{crews{id}}',
  );
  if (result.crews?.some((crew) => crew.id === id)) throw new Error('The crew deletion was not verified.');
  return { success: true, source: records.SOURCE, deleted: id };
}
module.exports = { saveCrew, deleteCrew };
