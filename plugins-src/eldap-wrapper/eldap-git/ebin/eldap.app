{application, eldap,
 [{description, "LDAP Client Library"},
  {vsn, "0.01"},
  {modules, [
    eldap,
    'ELDAPv3'
  ]},
  {registered, []},
  {applications, [kernel, stdlib]}
 ]}.
