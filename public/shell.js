/* ═══════════════════════════════════════════════════════════════
   YardDesk App Shell — Single source of truth for navigation,
   auth bootstrap, sidebar, topbar, and quick-create menu.

   Usage: Add before </body> on every internal page:
     <script src="shell.js"></script>

   Each page should have:
     <div class="main" id="main">
       <div class="topbar">...</div>  ← shell injects if missing
       <div class="page-content">...</div>
     </div>

   Shell auto-injects sidebar + topbar if not present.
   ═══════════════════════════════════════════════════════════════ */

(function() {
  'use strict';

  // ── Auth Bootstrap ──────────────────────────────────────────
  var t = localStorage.getItem('adminToken');
  if (!t) { window.location.href = '/login.html'; return; }
  var _fetch = window.fetch;
  window.fetch = function(url, opts) {
    opts = opts || {};
    if (typeof url === 'string' && url.startsWith('/api/')) {
      opts.headers = opts.headers || {};
      if (opts.headers instanceof Headers) { opts.headers.set('Authorization', 'Bearer ' + t); }
      else { opts.headers['Authorization'] = 'Bearer ' + t; }
    }
    return _fetch.call(this, url, opts).then(function(r) {
      if (r.status === 401 && typeof url === 'string' && url.startsWith('/api/')) {
        localStorage.removeItem('adminToken');
        localStorage.removeItem('adminName');
        window.location.href = '/login.html';
      }
      return r;
    });
  };

  // ── Navigation Definition ───────────────────────────────────
  // Jobber-style navigation: focused, clean, workflow-driven
  var NAV = [
    { href: 'index.html', icon: 'home', label: 'Home', perm: 'home' },
    { href: 'scheduling.html', icon: 'calendar', label: 'Schedule', perm: 'schedule' },
    { href: 'dispatch.html', icon: 'dispatch', label: 'Dispatch', perm: 'dispatch' },
    { href: 'customers.html', icon: 'clients', label: 'Clients', perm: 'clients' },
    { href: 'work-requests.html', icon: 'comms', label: 'Requests', perm: 'requests' },
    { href: 'quotes.html', icon: 'quotes', label: 'Quotes', perm: 'quotes' },
    { href: 'invoices.html', icon: 'invoices', label: 'Invoices', perm: 'invoices' },
    { href: 'expenses.html', icon: 'expenses', label: 'Expenses', perm: 'expenses' },
    'divider',
    { href: 'communications.html', icon: 'comms', label: 'Marketing', perm: 'marketing' },
    { href: 'reports.html', icon: 'reports', label: 'Reports', perm: 'reports' },
    { href: 'kpi.html', icon: 'kpi', label: 'Insights', perm: 'insights' },
    { href: 'crew.html', icon: 'crew', label: 'Crew', perm: 'crew' },
    { href: 'settings.html', icon: 'settings', label: 'Settings', perm: 'settings' }
  ];

  // ── Employee Permissions ────────────────────────────────────
  var isEmployee = localStorage.getItem('isEmployee') === 'true';
  var empPerms = null;
  try { empPerms = JSON.parse(localStorage.getItem('employeePermissions')); } catch(e) {}

  function hasPageAccess(permKey) {
    if (!isEmployee || !empPerms || !empPerms.pages) return true; // admins see everything
    var level = empPerms.pages[permKey];
    return level && level !== 'none';
  }

  function getPagePermLevel(permKey) {
    if (!isEmployee || !empPerms || !empPerms.pages) return 'full';
    return empPerms.pages[permKey] || 'none';
  }

  function getAdvancedPerm(key) {
    if (!isEmployee || !empPerms || !empPerms.advanced) return true;
    return !!empPerms.advanced[key];
  }

  // Expose for pages to check
  window.YardDesk = window.YardDesk || {};
  window.YardDesk.isEmployee = isEmployee;
  window.YardDesk.permissions = empPerms;
  window.YardDesk.hasPageAccess = hasPageAccess;
  window.YardDesk.getPagePermLevel = getPagePermLevel;
  window.YardDesk.getAdvancedPerm = getAdvancedPerm;

  // Pages accessible from parent pages (not in sidebar)
  // - sent-quotes.html, quote-generator.html, quote-calculator.html → from Quotes page
  // - payments.html → from Invoices page
  // - time-tracking.html → from Crew page
  // - communications.html, templates.html, campaigns.html, automations.html → from Settings
  // - pipeline.html → from Reports/Insights
  // - programs.html, cancellations.html, properties.html → from respective detail pages

  // ── SVG Icon Map ────────────────────────────────────────────
  var ICONS = {
    home: '<path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>',
    clients: '<path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4-4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/>',
    quotes: '<path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/>',
    send: '<path d="M22 2L11 13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/>',
    edit: '<path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 013 3L7 19l-4 1 1-4L16.5 3.5z"/>',
    calculator: '<rect x="4" y="2" width="16" height="20" rx="2"/><line x1="8" y1="6" x2="16" y2="6"/><line x1="8" y1="10" x2="10" y2="10"/><line x1="14" y1="10" x2="16" y2="10"/><line x1="8" y1="14" x2="10" y2="14"/><line x1="14" y1="14" x2="16" y2="14"/><line x1="8" y1="18" x2="16" y2="18"/>',
    calendar: '<rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/>',
    dispatch: '<polygon points="3 11 22 2 13 21 11 13 3 11"/>',
    jobs: '<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 00-2-2h-4a2 2 0 00-2 2v16"/>',
    invoices: '<rect x="1" y="4" width="22" height="16" rx="2" ry="2"/><line x1="1" y1="10" x2="23" y2="10"/>',
    payments: '<line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 000 7h5a3.5 3.5 0 010 7H6"/>',
    expenses: '<line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 000 7h5a3.5 3.5 0 010 7H6"/>',
    reports: '<line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/>',
    crew: '<path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4-4v2"/><circle cx="9" cy="7" r="4"/>',
    clock: '<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>',
    comms: '<path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/>',
    template: '<rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="9" y1="21" x2="9" y2="9"/>',
    campaign: '<path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22 6 12 13 2 6"/>',
    automation: '<path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/>',
    pipeline: '<polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/>',
    kpi: '<path d="M21.21 15.89A10 10 0 118 2.83"/><path d="M22 12A10 10 0 0012 2v10z"/>',
    settings: '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-2 2 2 2 0 01-2-2v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83 0 2 2 0 010-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 01-2-2 2 2 0 012-2h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 010-2.83 2 2 0 012.83 0l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 012-2 2 2 0 012 2v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 0 2 2 0 010 2.83l-.06.06a1.65 1.65 0 00-.33 1.82V9a1.65 1.65 0 001.51 1H21a2 2 0 012 2 2 2 0 01-2 2h-.09a1.65 1.65 0 00-1.51 1z"/>',
    plus: '<path d="M12 5v14M5 12h14"/>',
    search: '<circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>',
    bell: '<path d="M18 8A6 6 0 006 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 01-3.46 0"/>',
    menu: '<line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/>',
    customer: '<path d="M20 21v-2a4 4 0 00-4-4H8a4 4 0 00-4-4v2"/><circle cx="12" cy="7" r="4"/>',
    property: '<path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/>',
    message: '<path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/>'
  };

  function svg(name, size) {
    var s = size || 18;
    var inner = ICONS[name] || '';
    return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="'+s+'" height="'+s+'" style="width:'+s+'px;height:'+s+'px;flex-shrink:0">'+inner+'</svg>';
  }

  // ── Determine active page ───────────────────────────────────
  var path = window.location.pathname.split('/').pop() || 'index.html';

  // Sub-page → parent mapping (pages not in sidebar that should highlight a parent)
  var SUB_PAGES = {
    'sent-quotes.html': 'quotes.html',
    'sent-quote-detail.html': 'quotes.html',
    'quote-generator.html': 'quotes.html',
    'quote-calculator.html': 'quotes.html',
    'quote-detail.html': 'quotes.html',
    'customer-detail.html': 'customers.html',
    'new-customer.html': 'customers.html',
    'job-detail.html': 'scheduling.html',
    'new-job.html': 'scheduling.html',
    'invoice-detail.html': 'invoices.html',
    'new-invoice.html': 'invoices.html',
    'payments.html': 'invoices.html',
    'time-tracking.html': 'crew.html',
    'pipeline.html': 'kpi.html',
    'templates.html': 'communications.html',
    'campaigns.html': 'communications.html',
    'automations.html': 'communications.html',
    'finance.html': 'reports.html',
    'properties.html': 'customers.html',
    'programs.html': 'scheduling.html',
    'cancellations.html': 'scheduling.html',
    'import.html': 'settings.html',
    'import-scheduling.html': 'settings.html'
  };

  function isActive(href) {
    if (path === href) return true;
    // Check if current page is a sub-page of this nav item
    if (SUB_PAGES[path] === href) return true;
    return false;
  }

  function isParentActive(item) {
    return isActive(item.href);
  }

  // ── Build Sidebar HTML ──────────────────────────────────────
  function buildSidebar() {
    var html = '';

    // Brand
    html += '<div class="sidebar-brand">';
    html += '  <img src="/logo.png" alt="YardDesk">';
    html += '  <button class="sidebar-collapse-btn" id="shell-collapse-btn" title="Collapse sidebar">';
    html += '    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 17l-5-5 5-5"/><path d="M18 17l-5-5 5-5"/></svg>';
    html += '  </button>';
    html += '</div>';

    // Quick Create — Jobber-style: simple list, not a grid
    // Only show create items for pages the user has create/full access to
    var qcItems = [
      { href: 'new-customer.html', icon: 'customer', label: 'Client', perm: 'clients' },
      { href: 'quote-generator.html', icon: 'quotes', label: 'Quote', perm: 'quotes' },
      { href: 'new-job.html', icon: 'jobs', label: 'Job', perm: 'schedule' },
      { href: 'new-invoice.html', icon: 'invoices', label: 'Invoice', perm: 'invoices' },
      { href: 'properties.html?action=add', icon: 'property', label: 'Property', perm: 'clients' }
    ];
    var visibleQc = qcItems.filter(function(q) {
      var level = getPagePermLevel(q.perm);
      return level === 'create' || level === 'full';
    });
    if (visibleQc.length > 0) {
      html += '<div style="position:relative">';
      html += '  <button class="sidebar-new-btn" id="quick-create-btn" type="button">';
      html += '    ' + svg('plus') + '<span>Create</span>';
      html += '  </button>';
      html += '  <div class="qc-overlay" id="qc-overlay"></div>';
      html += '  <div class="quick-create-menu" id="quick-create-menu">';
      visibleQc.forEach(function(q) {
        html += '    <a href="' + q.href + '"><div class="qc-icon">' + svg(q.icon, 16) + '</div><span>' + q.label + '</span></a>';
      });
      html += '  </div>';
    } else {
      html += '<div style="position:relative">';
    }
    html += '</div>';

    // Nav items
    html += '<nav class="sidebar-nav">';
    for (var i = 0; i < NAV.length; i++) {
      var item = NAV[i];
      if (item === 'divider') {
        html += '<div class="sidebar-nav-divider"></div>';
        continue;
      }
      // Skip pages the employee has no access to
      if (item.perm && !hasPageAccess(item.perm)) continue;
      var active = isParentActive(item);
      html += '<a href="' + item.href + '" class="nav-item' + (active ? ' active' : '') + '">';
      html += svg(item.icon);
      html += '<span>' + item.label + '</span>';
      html += '</a>';
    }
    html += '</nav>';

    // Footer
    var userName = localStorage.getItem('adminName') || 'Admin';
    var userRole = isEmployee ? 'Employee' : 'Admin';
    var initials = userName.split(' ').map(function(w) { return w[0]; }).join('').toUpperCase().slice(0, 2);
    html += '<div class="sidebar-footer">';
    html += '  <div class="sidebar-user">';
    html += '    <div class="avatar">' + initials + '</div>';
    html += '    <div>';
    html += '      <div class="name">' + userName + '</div>';
    html += '      <div class="role">' + userRole + '</div>';
    html += '    </div>';
    html += '  </div>';
    html += '</div>';

    return html;
  }

  // ── Build Topbar HTML ───────────────────────────────────────
  function getPageTitle() {
    var titles = {
      'index.html': 'Home',
      'customers.html': 'Clients',
      'customer-detail.html': 'Client Details',
      'new-customer.html': 'New Client',
      'quotes.html': 'Quote Requests',
      'quote-detail.html': 'Quote Details',
      'quote-generator.html': 'Quote Generator',
      'quote-calculator.html': 'Quote Calculator',
      'sent-quotes.html': 'Sent Quotes',
      'sent-quote-detail.html': 'Sent Quote Details',
      'scheduling.html': 'Schedule',
      'dispatch.html': 'Dispatch Board',
      'work-requests.html': 'Requests',
      'job-detail.html': 'Job Details',
      'new-job.html': 'New Job',
      'invoices.html': 'Invoices',
      'invoice-detail.html': 'Invoice Details',
      'new-invoice.html': 'New Invoice',
      'payments.html': 'Payments',
      'expenses.html': 'Expenses',
      'reports.html': 'Reports',
      'crew.html': 'Crew',
      'time-tracking.html': 'Time Tracking',
      'communications.html': 'Communications',
      'templates.html': 'Templates',
      'campaigns.html': 'Campaigns',
      'automations.html': 'Automations',
      'pipeline.html': 'Pipeline',
      'kpi.html': 'KPIs',
      'settings.html': 'Settings',
      'finance.html': 'Finance',
      'properties.html': 'Properties',
      'email-log.html': 'Email Log',
      'calls.html': 'Calls',
      'programs.html': 'Programs',
      'cancellations.html': 'Cancellations',
      'import.html': 'Import',
      'import-scheduling.html': 'Import Schedule'
    };
    return titles[path] || document.title.replace(' — YardDesk', '');
  }

  function buildTopbar() {
    var html = '';
    html += '<div class="topbar-left">';
    html += '  <button class="mobile-menu-btn" id="shell-mobile-menu">';
    html += '    ' + svg('menu');
    html += '  </button>';
    html += '  <h1>' + getPageTitle() + '</h1>';
    html += '</div>';
    html += '<div class="topbar-actions">';
    html += '  <div class="topbar-search-wrap" id="shell-search-container">';
    html += '    <div class="topbar-search-icon">' + svg('search') + '</div>';
    html += '    <input type="text" class="topbar-search-input" id="shell-search-input" placeholder="Search customers, jobs, invoices..." autocomplete="off">';
    html += '    <div class="search-dropdown" id="shell-search-dropdown"></div>';
    html += '  </div>';
    html += '  <div style="position:relative">';
    html += '    <button class="topbar-icon-btn" title="Notifications" id="shell-notif-btn">';
    html += '      ' + svg('bell');
    html += '      <span class="notif-badge" id="shell-notif-badge" style="display:none"></span>';
    html += '    </button>';
    html += '    <div class="search-dropdown" id="shell-notif-dropdown" style="display:none;right:0;left:auto;min-width:340px;max-height:400px;">';
    html += '      <div style="padding:12px 14px;font-weight:600;font-size:14px;border-bottom:1px solid var(--gray-100);display:flex;justify-content:space-between;align-items:center;">';
    html += '        Notifications';
    html += '        <button id="shell-notif-mark-read" style="font-size:11px;color:var(--green);background:none;border:none;cursor:pointer;font-weight:600;">Mark all read</button>';
    html += '      </div>';
    html += '      <div id="shell-notif-list"><div class="search-empty">Loading...</div></div>';
    html += '    </div>';
    html += '  </div>';
    var userName = localStorage.getItem('adminName') || 'Admin';
    var initials = userName.split(' ').map(function(w) { return w[0]; }).join('').toUpperCase().slice(0, 2);
    html += '  <div style="position:relative">';
    html += '    <div class="topbar-avatar" title="Account" id="shell-avatar-btn" style="cursor:pointer">' + initials + '</div>';
    html += '    <div class="search-dropdown" id="shell-avatar-dropdown" style="display:none;right:0;left:auto;min-width:180px;">';
    html += '      <div style="padding:12px 14px;border-bottom:1px solid var(--gray-100);">';
    html += '        <div style="font-weight:600;font-size:13px;">' + userName + '</div>';
    html += '        <div style="font-size:11px;color:var(--gray-500);margin-top:2px;">' + (localStorage.getItem('adminEmail') || '') + '</div>';
    html += '      </div>';
    html += '      <a href="settings.html" style="display:block;padding:10px 14px;font-size:13px;color:var(--gray-700);text-decoration:none;" onmouseover="this.style.background=\'var(--gray-50)\'" onmouseout="this.style.background=\'none\'">Settings</a>';
    html += '      <div id="shell-logout-btn" style="padding:10px 14px;font-size:13px;color:#c0392b;cursor:pointer;border-top:1px solid var(--gray-100);" onmouseover="this.style.background=\'var(--gray-50)\'" onmouseout="this.style.background=\'none\'">Log Out</div>';
    html += '    </div>';
    html += '  </div>';
    html += '</div>';
    return html;
  }

  // ── Inject Shell ────────────────────────────────────────────
  function injectShell() {
    // Only inject if there's no sidebar already
    if (document.getElementById('sidebar')) return;

    var mainEl = document.querySelector('.main') || document.querySelector('.main-area') || document.querySelector('.main-content') || document.querySelector('main');
    if (!mainEl) {
      // Wrap page-content in .main if it doesn't exist
      var pc = document.querySelector('.page-content');
      if (pc) {
        mainEl = document.createElement('div');
        mainEl.className = 'main';
        mainEl.id = 'main';
        pc.parentNode.insertBefore(mainEl, pc);
        mainEl.appendChild(pc);
      } else {
        return; // Can't inject without structure
      }
    }
    // Ensure main has the right class for margin-left
    if (!mainEl.classList.contains('main')) {
      mainEl.classList.add('main');
    }

    // Create sidebar
    var aside = document.createElement('aside');
    aside.className = 'sidebar';
    aside.id = 'sidebar';
    aside.innerHTML = buildSidebar();
    document.body.insertBefore(aside, mainEl);

    // Inject topbar if missing
    var topbar = mainEl.querySelector('.topbar');
    if (!topbar) {
      topbar = document.createElement('div');
      topbar.className = 'topbar';
      topbar.innerHTML = buildTopbar();
      mainEl.insertBefore(topbar, mainEl.firstChild);
    }

    // Restore sidebar state
    var collapsed = localStorage.getItem('yd_sidebar_collapsed') === 'true';
    if (collapsed) aside.classList.add('collapsed');

    initShellEvents();
  }

  // ── Event Handlers ──────────────────────────────────────────
  function initShellEvents() {
    var sidebar = document.getElementById('sidebar');

    // Collapse toggle
    var collapseBtn = document.getElementById('shell-collapse-btn');
    if (collapseBtn) {
      collapseBtn.addEventListener('click', function() {
        sidebar.classList.toggle('collapsed');
        localStorage.setItem('yd_sidebar_collapsed', sidebar.classList.contains('collapsed'));
      });
    }

    // Mobile menu
    var mobileBtn = document.getElementById('shell-mobile-menu');
    if (mobileBtn) {
      mobileBtn.addEventListener('click', function() {
        sidebar.classList.toggle('open');
      });
    }

    // Quick Create menu
    var qcBtn = document.getElementById('quick-create-btn');
    var qcMenu = document.getElementById('quick-create-menu');
    var qcOverlay = document.getElementById('qc-overlay');
    if (qcBtn && qcMenu && qcOverlay) {
      qcBtn.addEventListener('click', function(e) {
        e.stopPropagation();
        qcMenu.classList.toggle('visible');
        qcOverlay.classList.toggle('visible');
      });
      qcOverlay.addEventListener('click', function() {
        qcMenu.classList.remove('visible');
        qcOverlay.classList.remove('visible');
      });
    }

    // Avatar / Logout dropdown
    var avatarBtn = document.getElementById('shell-avatar-btn');
    var avatarDd = document.getElementById('shell-avatar-dropdown');
    var logoutBtn = document.getElementById('shell-logout-btn');
    if (avatarBtn && avatarDd) {
      avatarBtn.addEventListener('click', function(e) {
        e.stopPropagation();
        avatarDd.style.display = avatarDd.style.display === 'none' ? 'block' : 'none';
      });
      document.addEventListener('click', function() { avatarDd.style.display = 'none'; });
      avatarDd.addEventListener('click', function(e) { e.stopPropagation(); });
    }
    if (logoutBtn) {
      logoutBtn.addEventListener('click', function() {
        localStorage.removeItem('adminToken');
        localStorage.removeItem('adminName');
        localStorage.removeItem('adminEmail');
        window.location.href = '/login.html';
      });
    }

    // Notifications
    initShellNotifications();

    // Global search
    initGlobalSearch();
  }

  // ── Notifications ──────────────────────────────────────────
  function initShellNotifications() {
    var btn = document.getElementById('shell-notif-btn');
    var dd = document.getElementById('shell-notif-dropdown');
    var markBtn = document.getElementById('shell-notif-mark-read');
    if (!btn || !dd) return;

    btn.addEventListener('click', function(e) {
      e.stopPropagation();
      if (dd.style.display === 'none') {
        dd.style.display = 'block';
        loadShellNotifications();
      } else {
        dd.style.display = 'none';
      }
    });

    document.addEventListener('click', function(e) {
      if (!dd.contains(e.target) && !btn.contains(e.target)) {
        dd.style.display = 'none';
      }
    });

    if (markBtn) {
      markBtn.addEventListener('click', function() {
        var badge = document.getElementById('shell-notif-badge');
        if (badge) badge.style.display = 'none';
        dd.style.display = 'none';
      });
    }

    // Load badge count on init
    loadShellNotifications(true);
  }

  function loadShellNotifications(badgeOnly) {
    var list = document.getElementById('shell-notif-list');
    var badge = document.getElementById('shell-notif-badge');
    var today = new Date().toISOString().split('T')[0];

    Promise.all([
      fetch('/api/invoices?status=sent&limit=10').then(function(r) { return r.json(); }).catch(function() { return { invoices: [] }; }),
      fetch('/api/work-requests?status=new&limit=5').then(function(r) { return r.json(); }).catch(function() { return { requests: [] }; }),
      fetch('/api/jobs?date=' + today + '&limit=5').then(function(r) { return r.json(); }).catch(function() { return { jobs: [] }; })
    ]).then(function(results) {
      var items = [];
      var now = new Date();

      (results[0].invoices || []).forEach(function(inv) {
        if (inv.due_date && new Date(inv.due_date) < now) {
          items.push({ icon: '\uD83D\uDCB0', text: 'Overdue invoice for ' + escapeHtml(inv.customer_name || 'Unknown'),
            sub: '$' + (Number(inv.total)||0).toFixed(2) + ' due ' + new Date(inv.due_date).toLocaleDateString(),
            href: 'invoice-detail.html?id=' + inv.id });
        }
      });

      (results[1].requests || []).forEach(function(r) {
        items.push({ icon: '\uD83D\uDCCB', text: 'New request from ' + escapeHtml(r.customer_name || r.name || 'Unknown'),
          sub: escapeHtml(r.service_type || r.description || '').slice(0, 60), href: 'work-requests.html' });
      });

      (results[2].jobs || []).forEach(function(j) {
        if (j.status === 'scheduled') {
          items.push({ icon: '\uD83D\uDDD3\uFE0F', text: escapeHtml(j.customer_name || 'Job') + ' \u2014 ' + escapeHtml(j.service_type || ''),
            sub: 'Scheduled for today', href: 'job-detail.html?id=' + j.id });
        }
      });

      if (badge) {
        if (items.length > 0) { badge.textContent = items.length > 9 ? '9+' : items.length; badge.style.display = ''; }
        else { badge.style.display = 'none'; }
      }

      if (badgeOnly || !list) return;

      if (!items.length) { list.innerHTML = '<div class="search-empty">You\'re all caught up!</div>'; return; }

      list.innerHTML = items.map(function(item) {
        return '<a href="' + item.href + '" class="search-result-item" style="gap:10px;padding:10px 14px;">'
          + '<div style="font-size:18px;flex-shrink:0;">' + item.icon + '</div>'
          + '<div style="min-width:0;">'
          + '<div style="font-size:13px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + item.text + '</div>'
          + '<div style="font-size:11px;color:var(--gray-400);">' + item.sub + '</div>'
          + '</div></a>';
      }).join('');
    });
  }

  // ── Global Search ───────────────────────────────────────────
  function initGlobalSearch() {
    var input = document.getElementById('shell-search-input');
    var dropdown = document.getElementById('shell-search-dropdown');
    if (!input || !dropdown) return;

    var debounceTimer;
    input.addEventListener('input', function() {
      clearTimeout(debounceTimer);
      var q = input.value.trim();
      if (q.length < 2) { dropdown.style.display = 'none'; return; }
      debounceTimer = setTimeout(function() { doGlobalSearch(q, dropdown); }, 300);
    });

    input.addEventListener('focus', function() {
      if (input.value.trim().length >= 2) dropdown.style.display = 'block';
    });

    document.addEventListener('click', function(e) {
      if (!input.contains(e.target) && !dropdown.contains(e.target)) {
        dropdown.style.display = 'none';
      }
    });

    // Keyboard shortcut: / to focus search
    document.addEventListener('keydown', function(e) {
      if (e.key === '/' && document.activeElement.tagName !== 'INPUT' && document.activeElement.tagName !== 'TEXTAREA') {
        e.preventDefault();
        input.focus();
      }
    });
  }

  function doGlobalSearch(query, dropdown) {
    var q = query.toLowerCase();

    // Search across customers, jobs, invoices in parallel
    Promise.all([
      fetch('/api/customers?search=' + encodeURIComponent(query) + '&limit=5').then(function(r) { return r.json(); }).catch(function() { return { customers: [] }; }),
      fetch('/api/jobs?search=' + encodeURIComponent(query) + '&limit=5').then(function(r) { return r.json(); }).catch(function() { return { jobs: [] }; }),
      fetch('/api/invoices?search=' + encodeURIComponent(query) + '&limit=5').then(function(r) { return r.json(); }).catch(function() { return { invoices: [] }; })
    ]).then(function(results) {
      var customers = (results[0].customers || []).slice(0, 5);
      var jobs = (results[1].jobs || []).slice(0, 5);
      var invoices = (results[2].invoices || []).slice(0, 5);

      if (!customers.length && !jobs.length && !invoices.length) {
        dropdown.innerHTML = '<div class="search-empty">No results for "' + query + '"</div>';
        dropdown.style.display = 'block';
        return;
      }

      var html = '';
      if (customers.length) {
        html += '<div class="search-section-label">Clients</div>';
        customers.forEach(function(c) {
          var name = c.name || ((c.first_name||'')+(c.last_name?' '+c.last_name:'')).trim() || 'Unknown';
          html += '<a class="search-result-item" href="customer-detail.html?id=' + c.id + '">';
          html += '<div class="search-result-icon" style="background:var(--green-bg);color:var(--green)">' + svg('clients', 14) + '</div>';
          html += '<div><div class="search-result-title">' + escapeHtml(name) + '</div>';
          if (c.email) html += '<div class="search-result-sub">' + escapeHtml(c.email) + '</div>';
          html += '</div></a>';
        });
      }
      if (jobs.length) {
        html += '<div class="search-section-label">Jobs</div>';
        jobs.forEach(function(j) {
          html += '<a class="search-result-item" href="job-detail.html?id=' + j.id + '">';
          html += '<div class="search-result-icon" style="background:var(--blue-bg);color:var(--blue)">' + svg('jobs', 14) + '</div>';
          html += '<div><div class="search-result-title">' + escapeHtml(j.customer_name || 'Job #' + j.id) + '</div>';
          html += '<div class="search-result-sub">' + escapeHtml(j.service_type || '') + '</div>';
          html += '</div></a>';
        });
      }
      if (invoices.length) {
        html += '<div class="search-section-label">Invoices</div>';
        invoices.forEach(function(inv) {
          html += '<a class="search-result-item" href="invoice-detail.html?id=' + inv.id + '">';
          html += '<div class="search-result-icon" style="background:var(--purple-bg);color:var(--purple)">' + svg('invoices', 14) + '</div>';
          html += '<div><div class="search-result-title">' + escapeHtml(inv.customer_name || 'Invoice #' + (inv.invoice_number || inv.id)) + '</div>';
          html += '<div class="search-result-sub">$' + (Number(inv.total)||0).toFixed(2) + ' - ' + (inv.status||'draft') + '</div>';
          html += '</div></a>';
        });
      }

      dropdown.innerHTML = html;
      dropdown.style.display = 'block';
    });
  }

  function escapeHtml(str) {
    if (!str) return '';
    return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  // ── Toast Utility (global) ──────────────────────────────────
  window.YardDesk = window.YardDesk || {};
  window.YardDesk.toast = function(msg, type) {
    var toast = document.getElementById('toast');
    if (!toast) {
      toast = document.createElement('div');
      toast.className = 'toast';
      toast.id = 'toast';
      document.body.appendChild(toast);
    }
    toast.textContent = msg;
    toast.className = 'toast ' + (type || 'success') + ' show';
    setTimeout(function() { toast.classList.remove('show'); }, 3000);
  };

  // ── Initialize ──────────────────────────────────────────────
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', injectShell);
  } else {
    injectShell();
  }

})();
