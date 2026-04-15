const jwt = require('jsonwebtoken');

function extractBearerToken(req) {
  const authHeader = req.headers['authorization'];
  if (!authHeader) return null;
  const [scheme, token] = authHeader.split(' ');
  if (scheme !== 'Bearer' || !token) return null;
  return token;
}

function decodeToken(token, jwtSecret) {
  return jwt.verify(token, jwtSecret);
}

function hasAdminAccess(user) {
  return Boolean(
    user &&
    (
      user.isAdmin === true ||
      user.accountType === 'admin' ||
      user.isServiceToken === true
    )
  );
}

function authenticateToken(jwtSecret) {
  return (req, res, next) => {
    const token = extractBearerToken(req);
    if (!token) return res.status(401).json({ message: 'Access token required' });

    try {
      req.user = decodeToken(token, jwtSecret);
      return next();
    } catch (error) {
      return res.status(403).json({ message: 'Invalid or expired token' });
    }
  };
}

module.exports = {
  authenticateToken,
  decodeToken,
  extractBearerToken,
  hasAdminAccess,
};
