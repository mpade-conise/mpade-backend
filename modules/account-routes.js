const registerAccountRoutes = ({ app, authenticateSupabaseUser }) => {
/* =========================================================
   ACCOUNT LIFECYCLE / SUPABASE ADMIN
========================================================= */

const requireSupabaseAdmin = (req, res, next) => {
  if (!process.env.SUPABASE_SERVICE_ROLE_KEY) {
    return res.status(503).json({
      error: 'Account lifecycle administration is not configured.',
      code: 'SUPABASE_ADMIN_NOT_CONFIGURED'
    });
  }

  return next();
};

const supabaseAdminRequest = async (method, pathname, body) => {
  const supabaseUrl = process.env.SUPABASE_URL?.replace(/\/+$/, '');
  if (!supabaseUrl || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error('Supabase admin configuration is missing.');
  }

  const response = await fetch(`${supabaseUrl}/auth/v1${pathname}`, {
    method,
    headers: {
      Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`,
      apikey: process.env.SUPABASE_SERVICE_ROLE_KEY,
      'Content-Type': 'application/json'
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) })
  });

  const text = await response.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = { message: text }; }

  if (!response.ok) {
    const error = new Error(data?.msg || data?.message || data?.error_description || 'Supabase admin request failed.');
    error.status = response.status;
    throw error;
  }

  return data;
};

const setAccountLifecycle = async (userId, status) => {
  if (status === 'active') {
    return supabaseAdminRequest('PUT', `/admin/users/${encodeURIComponent(userId)}`, {
      ban_duration: 'none',
      user_metadata: { account_status: 'active' }
    });
  }

  return supabaseAdminRequest('PUT', `/admin/users/${encodeURIComponent(userId)}`, {
    ban_duration: '876000h',
    user_metadata: { account_status: status }
  });
};

app.post('/api/account/deactivate', authenticateSupabaseUser, requireSupabaseAdmin, async (req, res) => {
  try {
    const userId = req.authUser.id;
    await setAccountLifecycle(userId, 'deactivated');

    return res.json({ success: true, status: 'deactivated' });
  } catch (error) {
    console.error('❌ Account deactivation error:', error.message);
    return res.status(error.status || 500).json({
      error: error.message || 'Unable to deactivate account.',
      code: 'ACCOUNT_DEACTIVATION_FAILED'
    });
  }
});

app.post('/api/account/reactivate', authenticateSupabaseUser, requireSupabaseAdmin, async (req, res) => {
  try {
    const userId = req.authUser.id;
    await setAccountLifecycle(userId, 'active');

    if (process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY) {
      const supabaseUrl = process.env.SUPABASE_URL.replace(/\/+$/, '');
      await fetch(`${supabaseUrl}/rest/v1/profiles?id=eq.${encodeURIComponent(userId)}`, {
        method: 'PATCH',
        headers: {
          Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`,
          apikey: process.env.SUPABASE_SERVICE_ROLE_KEY,
          'Content-Type': 'application/json',
          Prefer: 'return=minimal'
        },
        body: JSON.stringify({ account_status: 'active' })
      });
    }

    return res.json({ success: true, status: 'active' });
  } catch (error) {
    console.error('❌ Account reactivation error:', error.message);
    return res.status(error.status || 500).json({
      error: error.message || 'Unable to reactivate account.',
      code: 'ACCOUNT_REACTIVATION_FAILED'
    });
  }
});

app.delete('/api/account', authenticateSupabaseUser, requireSupabaseAdmin, async (req, res) => {
  try {
    const userId = req.authUser.id;
    const supabaseUrl = process.env.SUPABASE_URL.replace(/\/+$/, '');
    const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

    await supabaseAdminRequest('DELETE', `/admin/users/${encodeURIComponent(userId)}`);

    return res.json({ success: true, deleted: true });
  } catch (error) {
    console.error('❌ Account deletion error:', error.message);
    return res.status(error.status || 500).json({
      error: error.message || 'Unable to delete account.',
      code: 'ACCOUNT_DELETION_FAILED'
    });
  }
});

const requireB2 = (req, res, next) => {
  if (!b2Configured || !b2) {
    return res.status(503).json({
      error: 'Backblaze B2 storage is not configured.',
      code: 'B2_NOT_CONFIGURED'
    });
  }

  return next();
};

};

module.exports = { registerAccountRoutes };
