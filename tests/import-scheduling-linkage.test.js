jest.mock('../services/copilot/client', () => ({
  getCopilotToken: jest.fn(),
  fetchCopilotRouteJobsForDate: jest.fn(),
}));

const createJobRoutes = require('../routes/jobs');
const { getCopilotToken, fetchCopilotRouteJobsForDate } = require('../services/copilot/client');

function makeResponse() {
  return {
    statusCode: 200,
    body: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
  };
}

function findRoute(router, path, method) {
  return router.stack.find((layer) => layer.route?.path === path && layer.route.methods?.[method]);
}

async function invokeRoute(router, path, method, reqOverrides = {}) {
  const layer = findRoute(router, path, method);
  if (!layer) throw new Error(`Route not found: ${method.toUpperCase()} ${path}`);

  const req = {
    method: method.toUpperCase(),
    headers: {},
    query: {},
    body: {},
    user: null,
    get(name) {
      const key = Object.keys(this.headers).find((candidate) => candidate.toLowerCase() === String(name).toLowerCase());
      return key ? this.headers[key] : undefined;
    },
    ...reqOverrides,
  };
  const res = makeResponse();

  for (const stackItem of layer.route.stack) {
    let nextCalled = false;
    await new Promise((resolve, reject) => {
      const next = (error) => {
        nextCalled = true;
        if (error) reject(error);
        else resolve();
      };

      Promise.resolve(stackItem.handle(req, res, next))
        .then(() => {
          if (!nextCalled) resolve();
        })
        .catch(reject);
    });

    if (!nextCalled) break;
  }

  return res;
}

function csvBuffer(lines) {
  return Buffer.from(lines.join('\n'), 'utf8');
}

function makeServerError() {
  return jest.fn((res, error) => {
    res.status(500).json({
      success: false,
      error: error?.message || String(error),
    });
  });
}

describe('schedule import Copilot linkage', () => {
  let consoleLogSpy;
  let consoleWarnSpy;

  beforeEach(() => {
    jest.clearAllMocks();
    consoleLogSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleLogSpy.mockRestore();
    consoleWarnSpy.mockRestore();
  });

  test('legacy CSV scheduling is rejected without creating shadow jobs or reading browser data',async()=>{
 const pool={query:jest.fn()};const router=createJobRoutes({pool,serverError:makeServerError(),authenticateToken:(_req,_res,next)=>next(),upload:{single:()=> (_req,_res,next)=>next()}});
 const res=await invokeRoute(router,'/api/import-scheduling','post',{file:{buffer:csvBuffer(['customer_name,job_date','Jane,2026-04-20'])}});
 expect(res.statusCode).toBe(409);expect(res.body.destination).toBe('homeworks');expect(pool.query).not.toHaveBeenCalled();expect(fetchCopilotRouteJobsForDate).not.toHaveBeenCalled();
});

  ;
});
