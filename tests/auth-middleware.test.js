const jwt = require('jsonwebtoken');
const { authenticateToken, hasAdminAccess } = require('../middleware/auth');

describe('auth middleware helpers', () => {
  test('hasAdminAccess only allows admin-like tokens', () => {
    expect(hasAdminAccess({ isAdmin: true })).toBe(true);
    expect(hasAdminAccess({ accountType: 'admin' })).toBe(true);
    expect(hasAdminAccess({ isServiceToken: true })).toBe(true);
    expect(hasAdminAccess({ accountType: 'employee', isEmployee: true })).toBe(false);
    expect(hasAdminAccess({})).toBe(false);
  });

  test('authenticateToken accepts valid bearer tokens', () => {
    const middleware = authenticateToken('test-secret');
    const token = jwt.sign({ sub: '123', accountType: 'employee' }, 'test-secret');
    const req = { headers: { authorization: `Bearer ${token}` } };
    const res = { status: jest.fn().mockReturnThis(), json: jest.fn() };
    const next = jest.fn();

    middleware(req, res, next);

    expect(next).toHaveBeenCalled();
    expect(req.user.sub).toBe('123');
  });

  test('authenticateToken rejects missing bearer tokens', () => {
    const middleware = authenticateToken('test-secret');
    const req = { headers: {} };
    const res = { status: jest.fn().mockReturnThis(), json: jest.fn() };
    const next = jest.fn();

    middleware(req, res, next);

    expect(next).not.toHaveBeenCalled();
    expect(res.status).toHaveBeenCalledWith(401);
  });
});
