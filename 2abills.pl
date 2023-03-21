#!/usr/bin/perl -w

use strict;
use warnings;

=head1 NAME

  to ABillS migrations

   ebs (ExpertBilling)
   FreeNIBS
   Mabill
   UTM4
   UTM5 (mysql, Postgres)
   Mikbill
   Stargazer
   Nodeny
   Traffpro
   Lanbiling
   Easyhotspot
   Unisys
   BBilling
   Carbonsoft 4
   Carbonsoft 5
   NIkasyatem
   file
   Mikrobill


   2abills.pl former abills migration file for Cards module account creation

=head1 VERSION

  VERSION: 1.37
  UPDATE: 20230319

=cut

use DBI;
use strict;
use FindBin '$Bin';
use Encode;

my $argv = parse_arguments(\@ARGV);
my $VERSION = 1.37;
our (%conf);

#DB information
my $encryption_key = $argv->{PASSSWD_ENCRYPTION_KEY};

if (defined($argv->{'help'}) || $#ARGV < 0) {
  help();
  exit 0;
}

my $IMPORT_FILE   = $argv->{IMPORT_FILE} || '';
my $FILE_FIELDS   = $argv->{FILE_FIELDS} || '';
my $DEFAULT_PASSWORD = $argv->{DEFAULT_PASSWORD} || 'xxxx';
#my $email_export     = $ARGUMENTS->{EMAIL_CREATE}     || 1; #FIXME : missing functions
my $EMAIL_DOMAIN_ID = $argv->{EMAIL_DOMAIN} || 1;
my $DEBUG         = $argv->{DEBUG} || 0;
#my $no_deposit       = $ARGUMENTS->{NO_DEPOSIT}       || 0; #FIXME : missing functions
my $EXCHANGE_RATE = $argv->{EXCHANGE_RATE} || 0;
my $FORMAT        = ($argv->{'HTML'}) ? 'html' : '';
my $SYNC_DEPOSIT  = $argv->{SYNC_DEPOSIT} || 0;
my %EXTENDED_STATIC_FIELDS = ();
my $debug         = $argv->{DEBUG} || 0;

while (my ($k, $v) = each(%$argv)) {
  if ($k =~ /^(\d)\./) {
    $EXTENDED_STATIC_FIELDS{$k} = $v;
    print "Extended: $k -> $v\n" if ($DEBUG > 1);
  }
}

my DBI $db;

if ($argv->{ADD_NAS}) {
  add_nas();
  exit;
}

my $from = $argv->{FROM} || q{};

if ($from) {
  if ($from eq 'stargazer_pg') {
    $argv->{DB_TYPE} = 'Pg';
  }
  elsif ($from eq 'nika') {
    get_nika();
  }
  elsif ($from eq 'ODBC') {
    $argv->{DB_TYPE} = 'ODBC';
  }
  elsif ($from eq 'carbon4') {
    $argv->{DB_TYPE} = 'ODBC';
  }
  elsif ($from eq 'carbon5') {
    $argv->{DB_TYPE} = 'ODBC';
  }

  if($from ne 'file'){
    $db = db_connect({ %$argv });
  }
}

#Tarif migration section
my %TP_MIGRATION = ();
if ($argv->{TP_MIGRATION}) {
  my $rows = file_content($argv->{TP_MIGRATION});

  foreach my $line (@$rows) {
    my ($old, $new) = split(/=/, $line, 2);
    $TP_MIGRATION{$old} = $new;
  }
}

my $INFO_LOGINS;

if ($from) {
  if ($from eq 'freenibs') {
    $INFO_LOGINS = get_freenibs_users();
  }
  elsif ($from eq 'mabill') {
    $INFO_LOGINS = get_freenibs_users({ MABILL => 1 });
  }
  elsif ($from eq 'utm4') {
    $INFO_LOGINS = get_utm4_users();
  }
  elsif ($from eq 'utm5') {
    $INFO_LOGINS = get_utm5_users();
  }
  elsif($from eq 'custom_1') {
    get_custom_1();
  }
  elsif ($from eq 'utm5cards') {
    utm5cards();
    exit;
  }
  elsif ($from eq 'utm5pg') {
    $INFO_LOGINS = get_utm5pg_users();
  }
  elsif ($from eq 'unisys') {
    $INFO_LOGINS = get_unisys();
  }
  elsif ($from eq 'file') {
    if ($SYNC_DEPOSIT) {
      $FILE_FIELDS = 'LOGIN,NEW_SUM';
      $IMPORT_FILE = $SYNC_DEPOSIT;
      $INFO_LOGINS = get_file($argv);
      sync_deposit($INFO_LOGINS);
      exit 0;
    }
    else {
      $INFO_LOGINS = get_file($argv);
    }
  }
  elsif ($from eq 'abills') {
    $INFO_LOGINS = get_abills();
  }
  elsif ($from eq 'mikbill') {
    $INFO_LOGINS = get_mikbill({});
  }
  elsif ($from eq 'mikbill_deleted') {
    $INFO_LOGINS = get_mikbill({ DELETED => 1 });
  }
  elsif ($from eq 'mikbill_blocked') {
    $INFO_LOGINS = get_mikbill({ BLOCKED => 1 });
  }
  elsif ($from eq 'mikbill_freeze') {
    $INFO_LOGINS = get_mikbill({ FREEZE => 1 });
  }
  elsif ($from eq 'mikbill_payments') {
    mikbill_payments();
  }
  #  elsif ($from eq 'mikbill_deleted') {
  #    $INFO_LOGINS = get_mikbill_deleted();
  #  }
  #  elsif ($from eq 'mikbill_blocked') {
  #    $INFO_LOGINS = get_mikbill_blocked();
  #  }
  elsif ($from eq 'nodeny') {
    $INFO_LOGINS = get_nodeny();
  }
  elsif ($from eq 'traffpro') {
    $INFO_LOGINS = get_traffpro();
  }
  elsif ($from eq 'stargazer') {
    $INFO_LOGINS = get_stargazer();
  }
  elsif ($from eq 'stargazer_pg') {
    $INFO_LOGINS = get_stargazer_pg();
  }
  elsif ($from eq 'easyhotspot') {
    $INFO_LOGINS = get_easyhotspot();
  }
  elsif ($from eq 'bbilling') {
    $INFO_LOGINS = get_bbilling();
  }
  elsif ($from eq 'lms') {
    $INFO_LOGINS = get_lms();
  }
  elsif ($from eq 'lms_nodes') {
    $INFO_LOGINS = get_lms_nodes();
  }
  elsif ($from eq 'odbc') {
    $INFO_LOGINS = get_odbc();
  }
  elsif ($from eq 'carbon4') {
    $INFO_LOGINS = get_carbon4();
  }
  elsif ($from eq 'carbon5') {
    $INFO_LOGINS = get_carbon5();
  }
  elsif ($from eq 'mikrobill') {
    $INFO_LOGINS = get_mikrobill();
  }
  elsif ($from eq 'lanbilling') {
    $INFO_LOGINS = get_lanbilling();
  }
  elsif (defined(\&{ 'get_' . $from } )) {
    $INFO_LOGINS = &{\&{'get_'.$from}}();
  }
  elsif (defined(\&{$from})) {
    if($debug > 4) {
      print "Custom function: $from\n";
    }

    $INFO_LOGINS = &{\&$from}();
  }

  if($INFO_LOGINS) {
    show($INFO_LOGINS);
  }
}

if ($db) {
  $db->disconnect();
}

#**************************************************
=head2 db_connect($attr)

  Arguments:
    $attr
      DB_TYPE
      DB_NAME
      DB_USER
      DB_PASSWORD
      DB_PATH
      ABILLS_DB

  Returns:
    $db

=cut
#**************************************************
sub db_connect {
  my ($attr) = @_;

  my DBI $_db;

  my $dbhost = $attr->{DB_HOST} || "127.0.0.1";
  my $dbname = $attr->{DB_NAME} || "abills";
  my $dbuser = $attr->{DB_USER} || "root";
  my $dbpasswd = $attr->{DB_PASSWORD} || "";
  my $dbtype = $attr->{DB_TYPE} || "mysql"; #Pg
  my $dbpath = $attr->{DB_PATH} || ""; #carbon

  if ($attr->{ABILLS_DB}) {
    if (!-f '/usr/abills/libexec/config.pl') {
      print "Can't find /usr/abills/libexec/config.pl

ABillS not installed\n";

      exit;
    }

    do "/usr/abills/libexec/config.pl";

    $dbtype = $conf{dbtype};
    $dbhost = $conf{dbhost};
    $dbname = $conf{dbname};
    $dbuser = $conf{dbuser};
    $dbpasswd = $conf{dbpasswd};
  }
  elsif ($dbtype eq 'ODBC') {
    my $db_dsn = 'MSSQL';

    eval {require DBD::ODBC;};
    if ($@) {
      print "Please install 'DBD::ODBC'\n";
      print "Manual: http://abills.net.ua/wiki/doku.php/abills:docs:manual:soft:perl_odbc \n";
      exit;
    }
    DBD::ODBC->import();

    if ($from eq 'carbon4') {
      if (!$dbpath) {$dbpath = '/var/db/ics_main.gdb';}
      $_db = get_connection_to_firebird_host($dbhost, $dbpath, $dbpasswd);
    }
    elsif ($from eq 'carbon5') {
      if (!$dbpath) {$dbpath = '/var/db/billing.gdb';}
      $_db = get_connection_to_firebird_host($dbhost, $dbpath, $dbpasswd);
    }
    else {
      $_db = DBI->connect("dbi:$dbtype:DSN=$db_dsn;UID=$dbuser;PWD=$dbpasswd")
        or die "Unable connect to server '$dbtype:dbname=$dbname;host=$dbhost'\n user: $dbuser \n password: $dbpasswd \n$!\n"
        . ' dbname: ' . $dbname . ' dbuser: ' . $dbuser . ' dbpassword - ' . $dbpasswd
        . "\n" . $DBI::errstr . "\n" . $DBI::state . "\n" . $DBI::err . "\n" . $DBI::rows
        . "\nUSE:\n\n  DB_HOST=  DB_USER= DB_PASSWORD= DB_CHARSET= DB_NAME=  \n";
    }

    return $_db;
  }

  if ($DEBUG > 3) {
    print "DB info:
     dbtype: $dbtype
     dbhost: $dbhost
     dbname: $dbname
     dbuser: $dbuser
     dbpasswd: $dbpasswd
    ";
  }

  $_db = DBI->connect("dbi:$dbtype:dbname=$dbname;host=$dbhost", "$dbuser", "$dbpasswd")
    || die "Unable connect to server '$dbtype:dbname=$dbname;host=$dbhost'\n user: $dbuser \n"
    . "password: $dbpasswd \n$!\n" . ' dbname: ' . $dbname . ' dbuser: ' . $dbuser
    . ' dbpassword - ' . $dbpasswd . "\n" . $DBI::errstr . "\n" . $DBI::state . "\n"
    . $DBI::err . "\n" . $DBI::rows
    . "\nUSE:\n\n  DB_HOST=  DB_USER= DB_PASSWORD= DB_CHARSET= DB_NAME=  \n";

  if ($argv->{DB_CHARSET}) {
    $_db->do("SET NAMES $argv->{DB_CHARSET}");
  }

  return $_db;
}

#**************************************************
=head2

 ADD_NAS=file FIELDS=NAS_NAME,MAC

  Felds:
    NAS_NAME
    IP
   MAC
   NAS_IDENTIFIER
   NAS_DESCRIBE
   NAS_AUTH_TYPE
   NAS_MNG_IP_PORT
   NAS_MNG_USER
   NAS_MNG_PASSWORD
   NAS_RAD_PAIRS
   NAS_ALIVE
   NAS_DISABLE
   NAS_EXT_ACCT

=cut
#**************************************************
sub add_nas {
  #my ($attr) = @_;

  if (!$FILE_FIELDS) {
    print "Specify fields FILE_FIELDS=... \n";
    exit;
  }

  $FILE_FIELDS =~ s/\s//g;
  my @fiealds_arr = split(/,/, $FILE_FIELDS);

  my $arr = file_content($argv->{ADD_NAS});
  my @add_arr_hash = ();

  foreach my $line (@$arr) {
    print "$line\n" if ($DEBUG > 3);
    my @val_arr = split(/\t/, $line);

    my %val_hash = ();
    for (my $i = 0; $i <= $#val_arr; $i++) {
      $val_hash{ $fiealds_arr[$i] } = $val_arr[$i];
    }

    push @add_arr_hash, \%val_hash;
  }

  require Abills::SQL;
  Abills::SQL->import();

  require Nas;
  Nas->import();

  require Admins;
  Admins->import();

  require Dhcphosts;
  Dhcphosts->import();

  do "/usr/abills/libexec/config.pl";

  unshift(@INC, $Bin . '/../', $Bin . '/../Abills', $Bin . "/../Abills/$conf{dbtype}");

  if ($DEBUG > 3) {
    print "DB info:
     dbtype: $conf{dbtype}
     dbhost: $conf{dbhost}
     dbname: $conf{dbname}
     dbuser: $conf{dbuser}
     dbpasswd: $conf{dbpasswd}
    ";
  }

  my $abills_db = Abills::SQL->connect($conf{dbtype}, $conf{dbhost}, $conf{dbname}, $conf{dbuser},
    $conf{dbpasswd}, { CHARSET => ($conf{dbcharset}) ? $conf{dbcharset} : undef });

  my $admin = Admins->new($abills_db, \%conf);
  $admin->info($conf{SYSTEM_ADMIN_ID}, { IP => '127.0.0.1' });
  #my $Dhcphosts = Dhcphosts->new($abills_db, $admin, \%conf);
  $admin->{MODULE} = '';
  my $Nas = Nas->new($abills_db, \%conf);

  if ($DEBUG > 5) {
    return 0;
  }
  elsif ($DEBUG > 2) {
    $Nas->{debug} = 1;
  }

  for (my $i = 0; $i <= $#add_arr_hash; $i++) {
    if ($DEBUG > 2) {
      print "$i - ";
      for (my $if = 0; $if <= $#fiealds_arr; $if++) {
        print "$fiealds_arr[$if]:$add_arr_hash[$i]->{$fiealds_arr[$if]}  ";
      }
      print "\n";
    }

    if ($add_arr_hash[$i]->{MAC}) {
      $add_arr_hash[$i]->{MAC} = mac_former($add_arr_hash[$i]->{MAC});
    }

    $Nas->list({ NAS_IP => $add_arr_hash[$i]->{NAS_IP} || '0.0.0.0' });
    if ($Nas->{TOTAL}) {
      print "Exist add nas_identifier\n" if ($DEBUG > 3);
      $add_arr_hash[$i]->{NAS_IDENTIFIER} = 'NAS_' . +($Nas->{TOTAL} + 1);
    }

    $Nas->list($add_arr_hash[$i]);
    if ($Nas->{TOTAL}) {
      print "Skip: $fiealds_arr[0]:$add_arr_hash[$i]->{$fiealds_arr[0]} \n";
      next;
    }

    $Nas->add($add_arr_hash[$i]);
    if ($Nas->{errno}) {
      print "[$Nas->{errno}] $Nas->{errstr}\n";
      exit;
    }
  }

}

#**********************************************************
=head2 mac_former($mac);

=cut
#**********************************************************
sub mac_former {
  my ($mac) = @_;

  if (!$mac) {
    $mac = '00:00:00:00:00:00';
  }
  elsif ($mac =~ m/([0-9a-f]{2})([0-9a-f]{2})\.([0-9a-f]{2})([0-9a-f]{2})\.([0-9a-f]{2})([0-9a-f]{2})/i) {
    $mac = "$1:$2:$3:$4:$5:$6";
  }
  elsif ($mac =~ m/^([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i) {
    $mac = "00:$1:$2:$3:$4:$5";
  }
  elsif ($mac =~ m/([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})/i) {
    $mac = "$1:$2:$3:$4:$5:$6";
  }
  elsif ($mac =~ s/[\.\-]/:/g) {

  }

  return $mac;
}

#**************************************************
=head2 file_content($filename)

=cut
#**************************************************
sub file_content {
  my ($filename) = @_;

  my $content = '';

  open(my $fh, '<', $filename) || die "Can't open file '$filename' $! \n";
  while (<$fh>) {
    $content .= $_;
  }
  close($fh);

  my @arr = split(/[\r]?\n/, $content);

  return \@arr;
}

#**********************************************************
=head2 get_connection_to_firebird_host($server, $db_name, $password) - connect to remote Firebird DB

=cut
#**********************************************************
sub get_connection_to_firebird_host {
  my ($server, $db_name, $password) = @_;

  my $driver = "libOdbcFb.so"; # имя ODBC драйвера, скачан с официального сайта Firebird
  my $user = "sysdba";         # логин

  my %DSN = (
    ODBC => {
      dsn     => "Driver={$driver};DBNAME=$server:$db_name;uid=$user;pwd=$password",
      heading => 'Using ODBC Driver Manager via DBI::ODBC',
      mode    => 'ODBC',
    }
  );
  my $connect = "dbi:$DSN{ODBC}{'mode'}(RaiseError=>0, PrintError=>1, Taint=>0):$DSN{ODBC}{'dsn'}";

  #  print "Connect string : " . $connect . "\n";
  my $dbhandler = DBI->connect($connect, { PrintError => 1, AutoCommit => 0, ReadOnly => 1 }) || die("Can't connect: $DBI::errstr");

  $dbhandler->{LongReadLen} = 512 * 1024;
  $dbhandler->{LongTruncOk} = 1;

  return $dbhandler;
}

#**************************************************
=head2 get_abills()

=cut
#**************************************************
sub get_abills {
  #my ($attr) = @_;

  my %fields = (

    #DECODE(u.password, 'test12345678901234567890') AS password,
    #pi.fio as fio,
    #if(company.id IS NULL,b.deposit,cb.deposit) AS deposit,
    #if(u.company_id=0, u.credit, if (u.credit=0, company.credit, u.credit)) AS credit,
    #u.disable as disable,
    #u.company_id as company_id,
    #u.activate,
    #u.expire,
    #pi.phone,
    #pi.email,
    #pi.country_id,
    #pi.address_street,
    #pi.address_build,
    #pi.address_flat,
    #pi.comments,
    #pi.contract_id,
    #pi.contract_date,
    #pi.contract_sufix,
    #pi.pasport_num,
    #pi.pasport_date,
    #pi.pasport_grant,
    #pi.zip,
    #pi.city,
    #dv.tp_id,
    #INET_NTOA(dv.ip) AS ip,
    #dv.CID,
    #u.reduction,
    #u.gid,
    #u.registration,
    #pi.comments

    'LOGIN'            => 'id',
    'PASSWORD'         => 'password',
    '1.ACTIVATE'       => 'activate',
    '1.EXPIRE'         => 'expire',
    '1.COMPANY_ID'     => 'company_id',
    '1.CREDIT'         => 'credit',
    '1.GID'            => 'gid',
    '1.REDUCTION'      => 'reduction',
    '1.REGISTRATION'   => 'registration',
    '1.DISABLE'        => 'disable',
    '3.ADDRESS_FLAT'   => 'address_flat',
    '3.ADDRESS_STREET' => 'address_street',
    '3.ADDRESS_BUILD'  => 'address_build',
    '3.COUNTRY_ID'     => 'country_id',
    '3.COMMENTS'       => 'comments',
    '3.CONTRACT_ID'    => 'contract_id',
    '3.CONTRACT_DATE'  => 'contract_date',
    '3.CONTRACT_SUFIX' => 'contract_sufix',
    '3.EMAIL'          => 'email',
    '3.FIO'            => 'fio',
    '3.PHONE'          => 'phone',
    '3.ZIP'            => 'zip',
    '3.PHONE'          => 'phone',
    '3.CITY'           => 'city',
    '3.PASPORT_NUM'   => 'pasport_num',
    '3.PASPORT_DATE'  => 'pasport_date',
    '3.PASPORT_GRANT' => 'pasport_grant',

    '4.CID'            => 'CID',
    '4.FILTER_ID'      => 'filter_id',
    '4.IP'             => 'ip',

    #  '4.NETMASK'        => '\'255.255.255.255\'',
    #  '4.SIMULTANEONSLY' => 'simultaneous_use',
    #  '4.SPEED'          => 'speed',
    '4.TP_ID'          => 'tp_id',

    #  '4.CALLBACK'       => 'allow_callback',

    '5.SUM'            => 'deposit',
    '5.DESCRIBE'       => "'Migration'",

    #  '5.ER'             => undef,
    #  '5.EXT_ID'         => undef,

    #  '6.USERNAME'       => 'email',
    #  '6.DOMAINS_SEL'     => $email_domain_id || 0,
    #  '6.COMMENTS'        => '',
    #  '6.MAILS_LIMIT'	    => 0,
    #  '6.BOX_SIZE'	      => 0,
    #  '6.ANTIVIRUS'	      => 0,
    #  '6.ANTISPAM'	      => 0,
    #  '6.DISABLE'	        => 0,
    #  '6.EXPIRE'	        => undef,
    #  '6.PASSWORD'	      => 'email_pass',
  );

  my %fields_rev = reverse(%fields);
  #my $fields_list = "user, " . join(", \n", values(%fields));

  my $sql = "SELECT u.id, DECODE(u.password, 'test12345678901234567890') AS password,
  pi.fio as fio,
  if(company.id IS NULL,b.deposit,cb.deposit) AS deposit,
  if(u.company_id=0, u.credit, if (u.credit=0, company.credit, u.credit)) AS credit,
  u.disable as disable,
  u.company_id as company_id,
  u.activate,
  u.expire,
  pi.phone,
  pi.email,
  pi.country_id,
  pi.address_street,
  pi.address_build,
  pi.address_flat,
  pi.comments,
  pi.contract_id,
  pi.contract_date,
  pi.contract_sufix,
  pi.pasport_num,
  pi.pasport_date,
  pi.pasport_grant,
  pi.zip,
  pi.city,
  dv.tp_id,
  INET_NTOA(dv.ip) AS ip,
  dv.CID,
  u.reduction,
  u.gid,
  u.registration,
  pi.comments,
  u.id
   FROM users u
   LEFT JOIN users_pi pi ON (u.uid = pi.uid)
   LEFT JOIN bills b ON (u.bill_id = b.id)
   LEFT JOIN companies company ON (u.company_id=company.id)
   LEFT JOIN bills cb ON (company.bill_id=cb.id)
   LEFT JOIN dv_main dv ON (u.uid=dv.uid)
   GROUP BY u.id";
  print "$sql\n" if ($DEBUG > 0);

  # return 0;

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};
  #my $output       = '';
  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];
    $logins_hash{$LOGIN}{LOGIN} = $row[0];
    for (my $i = 1; $i < $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], $fields_rev{$query_fields->[$i]} -> $row[$i] \n";
      }
      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
    }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);
  return \%logins_hash;
}

#**************************************************
=head2 get_ebs()

=cut
#**************************************************
sub get_ebs {
  # |  | systemuser_id | entrance_code | private_passport_number | city_id |
  #   | promise_summ | allow_block_after_summ | block_after_summ | promise_days | promise_min_ballance | account_group_id
  #   | birthday | extra_fields | bonus_ballance | passportdislocate | disable_notifications

  my %fields = (
    #DECODE(u.password, 'test12345678901234567890') AS password,
    #pi.fio as fio,
    #if(company.id IS NULL,b.deposit,cb.deposit) AS deposit,
    #if(u.company_id=0, u.credit, if (u.credit=0, company.credit, u.credit)) AS credit,
    #u.disable as disable,
    #u.company_id as company_id,
    #u.activate,
    #u.expire,
    #pi.phone,
    #pi.email,
    #pi.country_id,
    #pi.address_street,
    #pi.address_build,
    #pi.address_flat,
    #pi.comments,
    #pi.contract_id,
    #pi.contract_date,
    #pi.contract_sufix,
    #pi.pasport_num,
    #pi.pasport_date,
    #pi.pasport_grant,
    #pi.zip,
    #pi.city,
    #dv.tp_id,
    #INET_NTOA(dv.ip) AS ip,
    #dv.CID,
    #u.reduction,
    #u.gid,
    #u.registration,
    #pi.comments
      'LOGIN'                 => 'a.username',
      'PASSWORD'              => "decrypt_pw(a.password, '" . ($argv->{PASSSWD_ENCRYPTION_KEY} || q{}) . "') AS password", # PGP encode
      # '1.ACTIVATE'       => 'activate',
      # '1.EXPIRE'         => 'expire',
      # '1.COMPANY_ID'     => 'company_id',
      '1.CREDIT'              => 'a.credit',
      '1.GID'                 => 'a.account_group_id',
      '1.DELETED'             => 'a.deleted',
      # '1.REDUCTION'      => 'reduction',
      '1.REGISTRATION'        => 'a.created',
      '1.DISABLE'             => 'a.status',
      '3.ADDRESS_FLAT'        => 'a.room',
      '3.DISTRICT'            => 'a.region',
      '3.ADDRESS_STREET'      => 'a.street',
      '3.CITY'                => 'a.city',
      '3.ZIP'                 => 'a.postcode',
      '3.ADDRESS_BUILD'       => 'a.house',
      '3._ADDRESS_HOUSE_BULK' => 'a.house_bulk',
      '3.FLOOR'               => 'a.row',
      '3.ENTRANCE'            => 'a.entrance',
      #'3.INN'            => 'a.'

      # '3.COUNTRY_ID'     => 'country_id',
      '3.COMMENTS'            => 'a.comment',
      '3.CONTRACT_ID'         => 'a.contract',
      # '3.CONTRACT_DATE'  => 'contract_date',
      # '3.CONTRACT_SUFIX' => 'contract_sufix',
      '3.EMAIL'               => 'a.email',
      '3.FIO'                 => 'a.fullname',
      #'3.FIO2'           => 'last_name',
      #'3.FIO3'           => 'first_name',
      '3.PHONE'               => 'a.phone_m', #,contactperson_phone,phone_h',
      '3.PASPORT_NUM'         => 'a.passport',
      '3.PASPORT_DATE'        => 'a.passport_date',
      '3.PASPORT_GRANT'       => 'a.passport_given',
      '3.TAX_NUMBER'          => 'a.private_passport_number',
      '3._HARDWARE_DATE'      => 'ah.datetime as _hardware_date',
      '3._PASPORT_REG'        => 'a.passportdislocate',

      '3._HARDWARE_MODEL_ID'  => 'h.model_id',
      '3._HARDWARE_NAME'      => 'h.name',
      '3._HARDWARE_SN'        => 'h.sn',
      '3._HARDWARE_COMMENT'   => 'ah.comment AS _hardware_comment',
      '3._HARDWARE_RETURNED'  => 'ah.returned',

      # '4.CID'            => 'CID',
      # '4.FILTER_ID'      => 'filter_id',
      # '4.IP'             => 'ip',
      '4.VLAN'                => 'a.vlan',
      '4.PASSWORD'            => "decrypt_pw(sa.password, '". ($argv->{PASSSWD_ENCRYPTION_KEY} || q{}) ."') AS internet_password",
    #
    # #  '4.NETMASK'        => '\'255.255.255.255\'',
    # #  '4.SIMULTANEONSLY' => 'simultaneous_use',
    # #  '4.SPEED'          => 'speed',
    # '4.TP_ID'          => 'tp_id',
    #
    # #  '4.CALLBACK'       => 'allow_callback',
    #
    '5.SUM'            => 'a.ballance',
    #'5.DESCRIBE'       => "'Migration'",

    #  '5.ER'             => undef,
    #  '5.EXT_ID'         => undef,

    #  '6.USERNAME'       => 'email',
    #  '6.DOMAINS_SEL'     => $email_domain_id || 0,
    #  '6.COMMENTS'        => '',
    #  '6.MAILS_LIMIT'	    => 0,
    #  '6.BOX_SIZE'	      => 0,
    #  '6.ANTIVIRUS'	      => 0,
    #  '6.ANTISPAM'	      => 0,
    #  '6.DISABLE'	        => 0,
    #  '6.EXPIRE'	        => undef,
    #  '6.PASSWORD'	      => 'email_pass',
    #'4.TP_NUM'         => 'ba.tarif_id AS tp_id',
    #'4.TP_NAME'        => 'tp.name AS tp_name',
    '4.TP_NUM'         => '(SELECT tarif_id FROM billservice_accounttarif ba, billservice_tariff tp
  WHERE ba.tarif_id=tp.id AND account_id=a.id AND datetime < NOW() ORDER BY datetime DESC  LIMIT 1) AS tp_num',
    '4.TP_NAME'        => '(SELECT tp.name FROM billservice_accounttarif ba, billservice_tariff tp
  WHERE ba.tarif_id=tp.id AND account_id=a.id AND datetime < NOW() ORDER BY datetime DESC LIMIT 1) AS tp_name',

    '4.CHANGE_TP_NAME' => 'tp_next.name AS next_tp_id',
    '4.CHANGE_TP_DATE' => 'next_tp.datetime AS next_tp_date',
    '4.IP'             => 'sa.vpn_ip_address',

  );

  my %fields_rev = reverse(%fields);
  my $fields_list = "a.username, " . join(", \n", values(%fields));

  foreach my $key (keys %fields_rev) {
    $key =~ /([a-z\_0-9]+)$/i;
    $fields_rev{$1}=$fields_rev{$key};
  }

  my $sql = "SELECT
     $fields_list
        FROM billservice_account a
        LEFT JOIN billservice_accounttarif ba ON (a.id=ba.account_id AND ba.datetime < NOW())
        LEFT JOIN billservice_tariff tp ON (ba.tarif_id=tp.id)
        LEFT JOIN billservice_subaccount sa ON (sa.account_id=a.id)
        LEFT JOIN billservice_accounthardware ah ON (ah.account_id=a.id)
        LEFT JOIN billservice_hardware h ON (h.id=ah.hardware_id)
        LEFT JOIN billservice_accounttarif next_tp ON (a.id=next_tp.account_id AND next_tp.datetime > NOW())
        LEFT JOIN billservice_tariff tp_next ON (next_tp.tarif_id=tp_next.id)
        WHERE
     tp.id <> 28
     AND a.status IN (1,4)
     AND a.deleted IS NULL
     GROUP BY a.id, ba.tarif_id, tp.name, next_tp.datetime, next_tp.tarif_id, model_id, h.name, ah.comment,
       sa.vpn_ip_address,
       h.sn, ah.returned, ah.datetime, sa.password, a.password,
       tp_next.name
  ;";

  print "$sql\n" if ($DEBUG > 0);

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};
  #my $output       = '';
  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];
    $logins_hash{$LOGIN}{LOGIN} = $row[0];
    for (my $i = 1; $i < $#row + 1; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], $fields_rev{$query_fields->[$i]} -> " . ($row[$i] || q{}) . "\n";
      }

      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i] || q{};
    }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }
  }

  undef($q);
  return \%logins_hash;
}

#**************************************************
=head2 utm5cards()

=cut
#**************************************************
sub utm5cards {
=comments

<?xml version="1.0" encoding="Windows-1251"?>
<UTM>
 <pool id='5' type='std'>
  <card id='11445' secret='817858938105' balance='25.0' currency='980' expire_date='31.12.2010' usage_date='01.09.2009' tp_id='0' />
  <card id='10810' secret='800909730456' balance='25.0' currency='980' expire_date='31.12.2010' usage_date='01.09.2009' tp_id='0' />
  <card id='20298' secret='167635967761' balance='25.0' currency='980' expire_date='31.12.2010' usage_date='��� ���' tp_id='0' />
  <card id='20297' secret='531258306760' balance='25.0' currency='980' expire_date='31.12.2010' usage_date='��� ���' tp_id='0' />
 </pool>
</UTM>

=cut

  if ($IMPORT_FILE eq '') {
    print "Select UTM5 Cards file\n";
    exit;
  }

  my $arr = file_content($IMPORT_FILE);

  do "/usr/abills/libexec/config.pl";

  shift(@$arr);
  my @sql_arr = ();
  foreach my $line (@$arr) {

    #print "$line\n";
    my @csv = split(/,/, $line);

    my $cards_id = $csv[0];
    my $secret = $csv[2];
    my $balance = $csv[3];
    my $expire_date = '';
    my $status = 0;
    if ($csv[5] =~ '(\d{2})\.(\d{2})\.(\d{4})') {
      $expire_date = "$3-$2-$1";
    }
    else {
      $expire_date = '0000-00-00';
    }

    if (length($csv[7]) > 6) {
      $status = 1;
    }

    #if ($line =~ /<card id='(\d+)'\s+secret='(\S+)' balance='([0-9\.\,]+)' currency='(\d+)' expire_date='(\d{2})\.(\d{2})\.(\d{4})' usage_date='��� ���' tp_id='0'/) {
    #my $cards_id = $1;
    #my $secret   = $2;
    #my $balance  = $3;
    #my $expire_date = "$7-$6-$5";
    push @sql_arr, "INSERT into cards_users (`serial`,`number`,`aid`, `expire`,`sum`, `pin`, `created`, `status`) values ('', '$cards_id', '1', '$expire_date', '$balance', ENCODE('$secret', '$conf{secretkey}'), now(), $status); ";

    #}
  }

  foreach my $line (@sql_arr) {
    print $line . "\n" if ($DEBUG > 1);
    if ($DEBUG < 5) {
      $db->do("$line");
    }
  }

  return 1;
}

#**************************************************
=head2 get_file($attr) Import from_TAB delimiter file

  Arguments:
    $attr
      CEL_DELIMITER

  Returns:

=cut
#**************************************************
sub get_file {
  my($attr)=@_;

  my @FILE_FIELDS = ('3.CONTRACT_ID', '3.FIO', 'LOGIN', 'PASSWORD', '3.ADDRESS_STREET', '4.IP', '3.COMMENTS', '5.SUM', '4.TP_ID', '3.PHONE',);

  @FILE_FIELDS = split(/,/, $FILE_FIELDS);

  if ($debug > 2) {
    print "File fields: ". join(',', @FILE_FIELDS) ."\n";
  }

  #$FILE_FIELDS[0] = 'LOGIN';
  #$FILE_FIELDS[1] = 'PASSWORD';
  my %logins_hash = ();
  my %TARIFS_HASH = ();
  my $TP_ID = 0;
  my $cel_delimiter = "\t";

  my $rows = file_content($IMPORT_FILE);

  if($attr->{CEL_DELIMITER}) {
    $cel_delimiter=$attr->{CEL_DELIMITER};
  }

  foreach my $line (@$rows) {
    my @cels = split(/$cel_delimiter/, $line);
    my %tmp_hash = ();
    my $COMMENTS = '';
    my $cel_phone = '';

    print $line if ($DEBUG > 4);

    for (my $i = 0; $i <= $#cels; $i++) {
      next if (!$FILE_FIELDS[$i]);
      #Tim quote
      $cels[$i] =~ s/^[\'\"]//;
      $cels[$i] =~ s/[\'\"]$//;
      my $field_name =  $FILE_FIELDS[$i] ;
      if($tmp_hash{ $field_name }) {
        $tmp_hash{ $field_name } .= $cels[$i];
      }
      else {
        $tmp_hash{ $field_name } = $cels[$i];
      }

      print "$i/$field_name - $cels[$i]\n" if ($DEBUG > 0);

      if ($tmp_hash{ $field_name } =~ /^(\d{2})[-.](\d{2})[-.](\d{4})$/) {
        $tmp_hash{ $field_name } = "$3-$2-$1";
      }

      if ($FILE_FIELDS[$i] eq '3.ADDRESS_STREET') {

        #     if($cels[$i] =~ /(.+), �. (.+)|(.+), �. (.+), k�. (.+)/) {
        #        $tmp_hash{'3.ADDRESS_STREET'}=$1;
        #        $tmp_hash{'3.ADDRESS_BUILD'}=$2;
        #        $tmp_hash{'3.ADDRESS_FLAT'}=$3;
        #        print "Street: $tmp_hash{'3.ADDRESS_STREET'} / $tmp_hash{'3.ADDRESS_BUILD'} /$tmp_hash{'3.ADDRESS_FLAT'} \n" if ($debug > 0);
        #        #exit;
        #      }
      }
      elsif ($field_name eq '5.SUM') {
        $tmp_hash{ $field_name } =~ s/,/./g;
        if ($EXCHANGE_RATE > 0) {
          $tmp_hash{ $field_name } = $tmp_hash{ $field_name } * $EXCHANGE_RATE;
        }
      }
      elsif($field_name eq '4.NAS_PORT') {
        my ($nas_ip, $port)=split(/#/, $tmp_hash{ $field_name });
        $tmp_hash{'4.NAS_IP'} = $nas_ip;
        $tmp_hash{'4.PORT'} = $port;
      }
      elsif ($field_name eq '4.TP') {

        #        if($tmp_hash{ $field_name } =~ /(\d+)/) {
        #          $tmp_hash{ $field_name } = $1;
        #        }
        if (!$TARIFS_HASH{ $tmp_hash{ $field_name } }) {
          $TP_ID += 10;
          $TARIFS_HASH{ $tmp_hash{ $field_name } } = $TP_ID;
        }

        $tmp_hash{'4.TP_NUM'} = $TARIFS_HASH{ $tmp_hash{ $field_name } };
        $tmp_hash{'4.TP_NAME'} = $tmp_hash{ $field_name };

      }
      elsif ($field_name eq '3.CONTRACT_ID') {
        $tmp_hash{ $field_name } =~ s/\-//g;
      }
      elsif ($field_name eq '4.IP' && $tmp_hash{ $field_name } =~ /([0-9\.]+)\/(\d+)/) {
        $tmp_hash{$field_name} = $1;
        my $bit_mask = $2;
        my $mask = 0b0000000000000000000000000000001;
        $tmp_hash{'4.NETMASK'}=int2ip(4294967296 - sprintf("%d", $mask << (32 - $bit_mask)));
      }
      elsif ($field_name eq '4.IP') {
        $tmp_hash{ $field_name } =~ /([0-9a-f]{1,3}\.[0-9a-f]{1,3}\.[0-9a-f]{1,3}\.[0-9a-f]{1,3})/g;
        $tmp_hash{ $field_name } = $1;
      }
      elsif ($field_name eq '3.PHONE') {
        $COMMENTS .= "PHONE: " . $tmp_hash{ $field_name };

        if ($tmp_hash{ $field_name } =~ s/_(.+)$//) {
          $cel_phone .= $1;
        }
        $tmp_hash{ $field_name } =~ s/,//g if ($tmp_hash{ $field_name });

        # print "$tmp_hash{$FILE_FIELDS[$i]} / $tmp_hash{'3._cel_phone'}\n";
      }
    }

    next if (!$tmp_hash{'LOGIN'});
    $tmp_hash{'3._cel_phone'} = $cel_phone if ($cel_phone);

    #$tmp_hash{'3.COMMENTS'}.=$COMMENTS;
    if ($tmp_hash{'F1'}) {
      $tmp_hash{'3.FIO'} .= "$tmp_hash{'F1'} $tmp_hash{'F2'} $tmp_hash{'F3'}";
    }

    $logins_hash{ $tmp_hash{'LOGIN'} } = \%tmp_hash;
    print "=====================\n" if ($DEBUG > 0);
  }

  if ($DEBUG > 1) {
    while (my ($k, $v) = each %TARIFS_HASH) {
      print "$k  -> $v\n";
    }

    exit if ($DEBUG > 5);
  }

  return \%logins_hash;
}

#**********************************************************
=head2 get_utm4_users

=cut
#**********************************************************
sub get_utm4_users {
  my %fields = (
    'LOGIN'            => 'login',
    'PASSWORD'         => 'password',

    #  '1.ACTIVATE'     => 'activated',
    #  '1.EXPIRE' 	     => 'expired',
    #  '1.COMPANY_ID'   => '',
    '1.CREDIT'         => 'credit',

    #  '1.GID' 	       => '',
    #  '1.REDUCTION'    => '',
    '1.REGISTRATION'   => 'DATE_FORMAT(FROM_UNIXTIME(reg_date), \'%Y-%m-%d\')',
    '1.DISABLE'        => 'block',

    #  '3.ADDRESS_FLAT'   => '',
    '3.ADDRESS_STREET' => 'actual_address',

    #  '3.ADDRESS_BUILD'  => '',
    #  '3.COMMENTS'       => '',
    #  '3.CONTRACT_ID' 	 => '',
    '3.EMAIL'          => 'email',
    '3.FIO'            => 'full_name',

    #  '3.PHONE'          => 'phone',

    #  '4.CID'            => '',
    #  '4.FILTER_ID'      => '',
    '4.IP'             => 'ip',

    #  '4.NETMASK'        => '\'255.255.255.255\'',
    #  '4.SIMULTANEONSLY' => 'simultaneous_use',
    #  '4.SPEED'          => 'speed',
    '4.TP_ID'          => 'tariff',

    #  '4.CALLBACK'       => 'allow_callback',

    '5.SUM'            => 'bill',

    #  '5.DESCRIBE' 	     => "'Migration'",
    #  '5.ER'             => undef,
    #  '5.EXT_ID'         => undef,

    #  '6.USERNAME'       => 'email',
    #  '6.DOMAINS_SEL'     => $email_domain_id || 0,
    #  '6.COMMENTS'        => '',
    #  '6.MAILS_LIMIT'	    => 0,
    #  '6.BOX_SIZE'	      => 0,
    #  '6.ANTIVIRUS'	      => 0,
    #  '6.ANTISPAM'	      => 0,
    #  '6.DISABLE'	        => 0,
    #  '6.EXPIRE'	        => undef,
    #  '6.PASSWORD'	      => 'email_pass',
  );

  my %fields_rev = reverse(%fields);

  #my $fields_list = "user, ". join(", \n", values(%fields));
  my $fields_list = "id, " . join(", \n", values(%fields));

  my $sql = "SELECT $fields_list FROM users";
  print "$sql\n" if ($DEBUG > 0);

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  #my $output      = '';
  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i < $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], $fields_rev{$query_fields->[$i]} -> $row[$i] \n";
      }

      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);
  return \%logins_hash;

}

#**********************************************************
=head2 get_utm5_users() Export from UTM5

=cut
#**********************************************************
sub get_utm5_users {

  my %fields = (
    'LOGIN'             => 'login',
    'PASSWORD'          => 'password',
    '1.ACTIVATE'        => 'IF(dp.start_date, DATE_FORMAT(FROM_UNIXTIME(dp.start_date), \'%Y-%m-%d\'), \'0000-00-00\')',
    '1.EXPIRE'          => 'IF(dp.end_date, DATE_FORMAT(FROM_UNIXTIME(dp.end_date), \'%Y-%m-%d\'), \'0000-00-00\')',

    #  '1.COMPANY_ID'    => '',
    '1.CREDIT'          => 'credit',
    '1.GID'             => 'IF(gl.group_id, gl.group_id, 0)',

    #  '1.REDUCTION'     => '',
    '1.REGISTRATION'    => 'FROM_UNIXTIME(u.create_date)',
    '1.DISABLE'         => 'IF(a.is_blocked, 1, 0)',

    '3.ADDRESS_FLAT'    => 'flat_number',
    '3.ADDRESS_STREET'  => 'IF(t12.street!=\'\', t12.street, \'\')',
    '3.ADDRESS_BUILD'   => 'IF(t12.number!=\'\', t12.number, \'\')',
    '3.COMMENTS'        => 'ua.comments',

    #  '3.CONTRACT_ID'       => '',
    '3.EMAIL'           => 'IF(u.email!=\'\', u.email, \'\')',
    '3.FIO'             => 'full_name',
    '3.PASPORT_GRANT'   => 'passport',
    '3.PHONE'           => 'home_telephone',
    '3.COUNTRY_ID'      => '804',
    '3.ZIP'             => 'IF(t12.post_code!=\'\', t12.post_code, \'\')',
    '3.CITY '           => 'IF(t12.city!=\'\', t12.city, \'\')',

    '3._entrance'       => 'IF(t12.building!=\'\', t12.building, \'\')',
    '3._work_telephone' => 'IF(u.work_telephone!=\'\', u.work_telephone, \'\')',
    '3._mobile'         => 'IF(u.mobile_telephone!=\'\', u.mobile_telephone, \'\')',
    '3._icq_number'     => 'IF(u.icq_number!=\'\', u.icq_number, \'\')',
    '3._web_page'       => 'IF(u.web_page!=\'\', u.web_page, \'\')',
    '3._old_id'         => 'IF(u.id, u.id, \'\')',
    '3._tariff_name'    => 'IF(t9.name!=\'\', t9.name, \'\')',
    '3._group_name'     => 'IF(t11.group_name!=\'\', t11.group_name, \'\')',

    '4.CID'             => 'IF(t1.mac!=\'\', t1.mac, \'\')',

    #  '4.FILTER_ID'      => '',
    '4.IP'              => 'IF(INET_NTOA(t1.ip&0xffffffff), INET_NTOA(t1.ip&0xffffffff), \'\')',

    #  '4.NETMASK'        => '\'255.255.255.255\'',
    #  '4.SIMULTANEONSLY' => 'simultaneous_use',
    #  '4.SPEED'          => 'speed',
    '4.TP_ID'           => 'IF(atl.tariff_id, atl.tariff_id, 0)',

    #  '4.CALLBACK'       => 'allow_callback',

    '5.SUM'             => 'balance',

    #  '5.DESCRIBE'       => "'Migration'",
    #  '5.ER'             => undef,
    #  '5.EXT_ID'         => undef,

    #  '6.USERNAME'       => 'email',
    #  '6.DOMAINS_SEL'     => $email_domain_id || 0,
    #  '6.COMMENTS'        => '',
    #  '6.MAILS_LIMIT'          => 0,
    #  '6.BOX_SIZE'       => 0,
    #  '6.ANTIVIRUS'              => 0,
    #  '6.ANTISPAM'       => 0,
    #  '6.DISABLE'          => 0,
    #  '6.EXPIRE'           => undef,
    #  '6.PASSWORD'       => 'email_pass',
  );

  my %fields_rev = reverse(%fields);
  my $fields_list = "u.login, " . join(", \n", values(%fields));

  my $sql = "SELECT $fields_list
  FROM (users u, users_accounts ua, accounts a)
  LEFT JOIN users_groups_link gl ON (u.id=gl.user_id)
  LEFT JOIN account_tariff_link atl ON (a.id=atl.account_id and atl.is_deleted=0)
  LEFT JOIN discount_periods dp ON (atl.discount_period_id=dp.id)
  LEFT JOIN user_contacts uc ON (u.id=uc.uid)
  LEFT JOIN service_links t3 ON (t3.user_id = u.id and t3.is_deleted <> '1')
  LEFT JOIN iptraffic_service_links t2 ON (t2.id = t3.id and t2.is_deleted <> '1')
  LEFT JOIN ip_groups t1 ON (t1.ip_group_id = t2.ip_group_id and t1.is_deleted <> '1')
  LEFT JOIN tariffs t9 ON (atl.tariff_id = t9.id and t9.is_deleted  <> '1')
  LEFT JOIN groups t11 ON (t11.id = gl.group_id)
  LEFT JOIN houses t12 ON (t12.id = u.house_id)

  WHERE u.id=ua.uid
    AND ua.account_id=a.id
    AND a.is_deleted=0
  GROUP BY u.login
  ORDER BY 1
";

  if ($DEBUG > 5) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  #my $output      = '';
  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i <= $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], " . $fields_rev{"$query_fields->[$i]"} . " -> $row[$i] \n";
      }

      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }
    # elsif ($logins_hash{$LOGIN}{'3.COMMENTS'}) {
    #   $logins_hash{$LOGIN}{'3.COMMENTS'} =~ s/\n//g;
    # }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }
  }

  undef($q);
  return \%logins_hash;
}

#**********************************************************
=head2 get_utm5pg_users() Export from UTM5

=cut
#**********************************************************
sub get_utm5pg_users {

  my %fields = (
    'LOGIN'            => 'login',
    'PASSWORD'         => 'password',
    '1.ACTIVATE'       => 'if(dp.start_date, DATE_FORMAT(FROM_UNIXTIME(dp.start_date), \'%Y-%m-%d\'), \'0000-00-00\')',
    '1.EXPIRE'         => 'if(dp.end_date, DATE_FORMAT(FROM_UNIXTIME(dp.end_date), \'%Y-%m-%d\'), \'0000-00-00\')',

    #  '1.COMPANY_ID'   => '',
    '1.CREDIT'         => 'credit',
    '1.GID'            => 'gid',

    #  '1.REDUCTION'    => '',
    '1.REGISTRATION'   => 'FROM_UNIXTIME(u.create_date)',
    '1.DISABLE'        => 'disable',

    '3.ADDRESS_FLAT'   => 'flat_number',
    '3.ADDRESS_STREET' => 'actual_address',
    '3.ADDRESS_BUILD'  => 'house_id',
    '3.COMMENTS'       => 'comments',

    #  '3.CONTRACT_ID'       => '',
    '3.EMAIL'          => 'if(u.email, u.email, \'\')',
    '3.FIO'            => 'full_name',
    '5.PASPORT_GRANT'  => 'passport',
    '3.PHONE'          => 'home_telephone',

    #  '4.CID'            => '',
    #  '4.FILTER_ID'      => '',
    #  '4.IP'             => 'ip',
    #  '4.NETMASK'        => '\'255.255.255.255\'',
    #  '4.SIMULTANEONSLY' => 'simultaneous_use',
    #  '4.SPEED'          => 'speed',
    '4.TP_ID'          => 'tp', #'COALESCE(atl.tariff_id, 0)',

    #  '4.CALLBACK'       => 'allow_callback',

    '5.SUM'            => 'balance',

    #  '5.DESCRIBE'       => "'Migration'",
    #  '5.ER'             => undef,
    #  '5.EXT_ID'         => undef,

    #  '6.USERNAME'       => 'email',
    #  '6.DOMAINS_SEL'     => $email_domain_id || 0,
    #  '6.COMMENTS'        => '',
    #  '6.MAILS_LIMIT'          => 0,
    #  '6.BOX_SIZE'       => 0,
    #  '6.ANTIVIRUS'              => 0,
    #  '6.ANTISPAM'       => 0,
    #  '6.DISABLE'          => 0,
    #  '6.EXPIRE'           => undef,
    #  '6.PASSWORD'       => 'email_pass',
  );

  my %fields_rev = reverse(%fields);

  #my $fields_list = "u.login, ". join(", \n", values(%fields));
  #print $fields_list;
  #my $sql = "select $fields_list
  # FROM users u, users_accounts ua, accounts a
  #  LEFT JOIN users_groups_link gl ON (u.id=gl.user_id)
  #  LEFT JOIN account_tariff_link atl ON (a.id=atl.account_id and atl.is_deleted=0)
  #  LEFT JOIN discount_periods dp ON (atl.discount_period_id=dp.id)
  #  LEFT JOIN user_contacts uc ON (u.id=uc.uid)
  #  WHERE u.id=ua.uid
  #  and ua.account_id=a.id
  #  ORDER BY 1
  #";

  my $sql = "select  u.login, full_name, COALESCE(to_char(TIMESTAMP WITH TIME ZONE 'epoch' + dp.end_date * interval '1 second', 'YYYY-MM-DD'), '0000-00-00'),
password, balance,  comments,
COALESCE(to_char(TIMESTAMP WITH TIME ZONE 'epoch' + dp.start_date * interval '1 second', 'YYYY-MM-DD'), '0000-00-00'),
house_id, TIMESTAMP WITH TIME ZONE 'epoch' + u.create_date * interval '1 second', login, COALESCE(u.is_blocked, 0) as disable, credit,
passport,  COALESCE(u.email, '') as email, flat_number,  actual_address,	COALESCE(gl.group_id, 0) as gid,
COALESCE(atl.tariff_id, 0) AS TP, full_name, home_telephone
FROM accounts a  INNER JOIN users_accounts ua ON a.id = ua.account_id
INNER JOIN users u ON ua.uid = u.id
LEFT JOIN users_groups_link gl ON (u.id=gl.user_id)
LEFT JOIN account_tariff_link atl ON (a.id=atl.account_id and atl.is_deleted=0)
LEFT JOIN discount_periods dp ON (atl.discount_period_id=dp.id)
LEFT JOIN user_contacts uc ON (u.id=uc.uid)
ORDER BY u.login";

  if ($DEBUG > 4) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  #my $output      = '';
  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i < $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], " . $fields_rev{"$query_fields->[$i]"} . " -> $row[$i] \n";
      }

      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);
  return \%logins_hash;
}

#**********************************************************
#
#**********************************************************
sub get_freenibs_users {
  my ($attr) = @_;

  my %fields = (
    'LOGIN'            => 'user',
    'PASSWORD'         => 'password',

    #  '1.ACTIVATE'     => 'activated',
    '1.EXPIRE'         => 'expired',

    #  '1.COMPANY_ID'   => '',
    '1.CREDIT'         => 'credit',

    #  '1.GID' 	       => '',
    #  '1.REDUCTION'    => '',
    '1.REGISTRATION'   => 'add_date',
    '1.DISABLE'        => 'blocked',

    #  '3.ADDRESS_FLAT'   => '',
    '3.ADDRESS_STREET' => 'address',

    #  '3.ADDRESS_BUILD'  => '',
    #  '3.COMMENTS'       => '',
    #  '3.CONTRACT_ID' 	 => '',
    #  '3.EMAIL'          => '',
    '3.FIO'            => 'fio',
    '3.PHONE'          => 'phone',

    #  '4.CID'            => '',
    #  '4.FILTER_ID'      => '',
    '4.IP'             => 'framed_ip',

    #  '4.NETMASK'        => '\'255.255.255.255\'',
    '4.SIMULTANEONSLY' => 'simultaneous_use',
    '4.TP_ID'          => 'gid',
    '4.CALLBACK'       => 'allow_callback',

    '5.SUM'            => 'deposit',

    #  '5.DESCRIBE' 	     => "'Migration'",
    #  '5.ER'             => undef,
    #  '5.EXT_ID'         => undef,

  );

  if ($attr->{MABILL}) {
    $fields{'4.SPEED'} = 'speed';
    $fields{'6.USERNAME'} = 'email', $fields{'6.DOMAINS_SEL'} = $EMAIL_DOMAIN_ID || 0;
    $fields{'PASSWORD'} = 'if(crypt_method=1, email_pass, password)';

    #  '6.COMMENTS'        => '',
    #  '6.MAILS_LIMIT'	    => 0,
    #  '6.BOX_SIZE'	      => 0,
    #  '6.ANTIVIRUS'	      => 0,
    #  '6.ANTISPAM'	      => 0,
    #  '6.DISABLE'	        => 0,
    #  '6.EXPIRE'	        => undef,
    $fields{'6.PASSWORD'} = 'email_pass';
  }

  my %fields_rev = reverse(%fields);
  my $fields_list = "user, " . join(", \n", values(%fields));

  my $sql = "SELECT $fields_list FROM users";
  print "$sql\n" if ($DEBUG > 0);

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  #my $output = '';
  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i < $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], $fields_rev{$query_fields->[$i]} -> $row[$i] \n";
      }

      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);

  return \%logins_hash;
}

#**********************************************************
=head2 all_columns($logins_info) - Returns all used columns 

  Arguments:
    $logins_info (HASH_REF)

  Returns: 
    @columns
=cut
#**********************************************************
sub all_columns {
  my ($logins_info) = @_;
  my %columns;
  foreach my $login (values %$logins_info){
    foreach(sort keys %$login){
      $columns{$_}=1;
    }
  };
  return (sort keys %columns);
}

#**********************************************************
=head2 show($logins_info)

  Arguments:
    $logins_info (HASH_REF)

  Returns:

=cut
#**********************************************************
sub show {
  my ($logins_info) = @_;

  my $counts = 0;
  my $output = '';

  print "Output format: $FORMAT\n" if ($DEBUG > 1);

  my %exaption = (
    'LOGIN'    => 1,
    'PASSWORD' => 2
  );

  if ($argv->{SKIP_FIELDS}) {
    foreach my $field (split(/,\s?/, $argv->{SKIP_FIELDS})) {
      $exaption{$field}=1;
    }
  }

  my @titles = sort keys %$logins_info;

  if ($#titles == -1) {
    print "Error: No input data\n";
    return 0;
  }

  my $login = $titles[0];

  @titles = all_columns($logins_info);

  if ($argv->{LOGIN2UID} && !$logins_info->{$login}{'1.UID'}) {
    push @titles, '1.UID';
  }

  if ($FORMAT eq 'html') {
    if ($argv->{HEADER}) {
      $output = $argv->{HEADER};
    }

    $output .= "<table border=1>\n" . "<tr><th>LOGIN</th>
	<th>PASSWORD</th>\n";

    foreach my $column_title (@titles) {
      next if ($exaption{$column_title});
      $output .= "<th>$column_title</th>\n";
    }
    $output .= "</tr>\n";
  }

  foreach my $login_ (sort keys %$logins_info) {

    #add login to uid
    if ($argv->{LOGIN2UID} && !$logins_info->{$login_}{'1.UID'}) {
      $logins_info->{$login_}{'1.UID'} = $login_;
    }

    next if (!$login_);
    print "$login_\n" if ($DEBUG > 0);
    $logins_info->{$login_}{'LOGIN'} =~ s/\s//g;

    if ($FORMAT eq 'html') {
      my $password = $logins_info->{$login_}{'PASSWORD'} || $DEFAULT_PASSWORD;
      $output .= "<tr><td>$logins_info->{$login_}{'LOGIN'}</td><td>$password</td>";
      my $col_number = 0;
      foreach my $column_title (@titles) {
        $col_number++;
        if (!$column_title) {
          print "Column: $col_number " . ($column_title || "empty title\n");
          print join("\n", @titles);
          print "\n";
          exit;
        }

        next if ($exaption{$column_title});
        my $value = q{};
        if (defined($logins_info->{$login_}{$column_title})) {
          $value = $logins_info->{$login_}{$column_title};
        }
        if ($argv->{win2utf}) {
          $value = Encode::encode('utf8', Encode::decode('cp1251', $value));
        }

        if ($column_title =~ /CONTRACT_DATE|REGISTRATION|PASPORT_DATE|DATE/) {
          $value = _date_convert($value);
        }

        $value =~ s/NULL//g;

        $output .= "<td>" . $value . "</td>";
      }
      $output .= "</tr>\n";
    }
    else {

      $output .= "$logins_info->{$login_}{'LOGIN'}\t" . (($logins_info->{$login_}{'PASSWORD'}) ? $logins_info->{$login_}{'PASSWORD'} : '-') . "\t";

      foreach my $column_title (@titles) {
        next if ($exaption{$column_title});

        my $value = $logins_info->{$login_}{$column_title} || q{};
        if ($argv->{win2utf}) {
          $value = Encode::encode('utf8', Encode::decode('cp1251', $value));
        }

        if ($column_title eq '4.TP_ID' && $TP_MIGRATION{ $value }) {
          $value = $TP_MIGRATION{ $value };
        }

        if ($column_title =~ /CONTRACT_DATE|REGISTRATION|PASPORT_DATE|DATE/) {
          $value = _date_convert($value);
        }
        elsif ($column_title =~ /COMMENTS/) {
          $value =~ s/[\r\n]+/ /g;
        }
        elsif ($column_title eq '1.CREDIT' && (! $value || $value !~ /\d+/)) {
          next;
        }
        elsif ($column_title eq '5.SUM' && (! $value || $value !~ /\d+/)) {
          next;
        }
        elsif ($column_title eq '1.DISABLE' && (! $value || $value !~ /^\d+$/)) {
          next;
        }

        $value =~ s/NULL//g if ($value);

        if (! $value) {
          next;
        }

        #Address full
        if ($column_title eq '3.ADDRESS_FULL') {
          if ($argv->{ADDRESS_DELIMITER}) {
            my ($delimiter1, $delimiter2) = split(/,/, $argv->{ADDRESS_DELIMITER}, 2);

            if (!$delimiter2) {
              $delimiter2 = '';
            }

            if ($argv->{win2utf}) {
              $value = Encode::encode('utf8', Encode::decode('cp1251', $value));
            }

            $value =~ m/(.+)$delimiter1(.+)$delimiter2(.{0,10})/;

            my ($ADDRESS_STREET, $ADDRESS_BUILD, $ADDRESS_FLAT) = ($1, $2, $3);

            if ($ADDRESS_STREET) {
              $output .= qq{3.ADDRESS_STREET="$ADDRESS_STREET"\t};
            }
            else {
              $output .= qq{3.ADDRESS_STREET="$value"\t};
            }

            if ($ADDRESS_BUILD) {
              $output .= qq{3.ADDRESS_BUILD="$ADDRESS_BUILD"\t};
            }

            if ($ADDRESS_FLAT) {
              $output .= qq{3.ADDRESS_FLAT="$3"\t};
            }

            next;
          }
        }

        #print "$login $column_title\n" if(! $logins_info->{$login}{$column_title});
        if ($logins_info->{$login_}{$column_title}) {
          $output .= "$column_title=\"" . $value . "\"\t";
        }
      }

      if ($argv->{SKIP_ERROR_PARAM}) {
        $output .= "SKIP_ERRORS=1\t4.INTERNET_SKIP_FEE=1\t9.SKIP_FEE\t";
      }

      if ($argv->{ADD_PARAMS}) {
        $argv->{ADD_PARAMS} =~ s/,/\t/g;
        $output .= "$argv->{ADD_PARAMS}\t";
      }

      $output .= "\n";
    }

    $counts++;
  }

  if ($FORMAT eq 'html') {
    $output .= "</table>\n";
  }

  print "$output\n";
  print "ROWS: $counts\n";

  return 1;
}

#*******************************************************************
=head2 get_unisys() -  Parse comand line arguments

=cut
#*******************************************************************
sub get_unisys {

  my %fields = (
    'LOGIN'            => 'name',
    'PASSWORD'         => '\'-\'',
    '1.ACTIVATE'       => 'DATE_FORMAT(allowsince, \'Y-%m-%d\')',
    '1.EXPIRE'         => 'DATE_FORMAT(allowsince, \'Y-%m-%d\')',

    #  '1.COMPANY_ID'   => '',
    #  '1.CREDIT'         => '',
    #  '1.GID'              => 'if(gl.group_id, gl.group_id, 0)',
    #  '1.REDUCTION'    => '',
    '1.REGISTRATION'   => 'since',

    #  '1.DISABLE'      => 'if(u.is_blocked, u.is_blocked, 0)',

    #  '3.ADDRESS_FLAT'   => 'address',
    '3.ADDRESS_STREET' => 'address',

    #  '3.ADDRESS_BUILD'  => 'house_id',
    '3.COMMENTS'       => 'rem',

    #  '3.CONTRACT_ID'       => '',
    '3.EMAIL'          => 'email',
    '3.FIO'            => 'fullname',

    #  '5.PASPORT_GRANT'  => 'passport',
    '3.PHONE'          => 'phone',

    #  '4.CID'            => '',
    #  '4.FILTER_ID'      => '',
    #  '4.IP'             => 'ip',
    #  '4.NETMASK'        => '\'255.255.255.255\'',
    #  '4.SIMULTANEONSLY' => 'simultaneous_use',
    #  '4.SPEED'          => 'speed',
    #  '4.TP_ID'          => 'if(atl.tariff_id, atl.tariff_id, 0)',
    #  '4.CALLBACK'       => 'allow_callback',

    '5.SUM'            => 'balance',

    #  '5.DESCRIBE'       => "'Migration'",
    #  '5.ER'             => undef,
    #  '5.EXT_ID'         => undef,

    #  '6.USERNAME'       => 'email',
    #  '6.DOMAINS_SEL'     => $email_domain_id || 0,
    #  '6.COMMENTS'        => '',
    #  '6.MAILS_LIMIT'          => 0,
    #  '6.BOX_SIZE'       => 0,
    #  '6.ANTIVIRUS'              => 0,
    #  '6.ANTISPAM'       => 0,
    #  '6.DISABLE'          => 0,
    #  '6.EXPIRE'           => undef,
    #  '6.PASSWORD'       => 'email_pass',
  );

  my %fields_rev = reverse(%fields);
  my $fields_list = "name, " . join(", \n", values(%fields));

  my $sql = "select $fields_list
  FROM users
  ORDER BY 1
";

  if ($DEBUG > 4) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  #my $output      = '';
  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i < $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], " . $fields_rev{"$query_fields->[$i]"} . " -> $row[$i] \n";
      }

      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);
  return \%logins_hash;

}

#*******************************************************************
# Parse comand line arguments
# help(@$argv)
#*******************************************************************
sub help {

  print <<"[END]";
ABillS Migration system Version: $VERSION
(http://abills.net.ua)

  Options:
    DEFAULT_PASSWORD    - default  password for empty passwords
    PASSSWD_ENCRYPTION_KEY - Password encryption key
    EMAIL_CREATE        - create email accounts
    EMAIL_DOMAIN        - ABillS E-mail domain ( CHECK '/ System configuration/ E-MAIL/ Domains/' )
    DEBUG               - Enable debug
    ADDRESS_DELIMITER=  - Addreess delimeter for field 3.ADDRESS_FULL (Address delimiter street_name[delimeter1]build[delimiter2][flat])
                          ADDRESS_DELIMITER="[delimiter1],[delimiter2]"
    SKIP_ERROR_PARAM=1  - Skip error and fees (Add:  SKIP_ERRORS=1	4.DV_SKIP_FEE=1)
    ADD_PARAMS=         - Add ext params with coma delimeter (ADD_PARAMS="1.GID=1000,5.STATUS=5")
    NO_DEPOSIT          - Don\'t transfer deposit
    FROM                - Migration From:
                            freenibs
                            mabill
                            utm4
                            utm5
                            utm5pg
                            file      - Tab delimiter file
                            utm5cards - require IMPORT_FILE paraments with utm cards
                            abills    - get users from another abills
                            mikbill - get users from mikbill
                              mikbill_deleted - get deleted users from mikbill
                              mikbill_blocked - get blocked users from mikbill
                            nodeny
                           	traffpro
                            stargazer    - MySQL DB
                            stargazer_pg - stargazer Postgree DB
                            carbon4
                            carbon5
                            lms
                            lms_nodes (IP, MAC adresses for lms users)
                            odbc
                            nika
                            mikrobill
    SYNC_DEPOSIT        -  filename to sync deposit ( ./2abills.pl FROM=file SYNC_DEPOSIT=/usr/deposits )
    IMPORT_FILE=[file]  - Tab delimiter file or CEL_DELIMITER=...
    CEL_DELIMITER       - Cel delimiter for file (Default: TAB)
    FILE_FIELDS=[list,.]- Tab delimiter fields position (FILE_FIELDS=LOGIN,PASSWORD,3.FIO...)
    TP_MIGRATION=[file] - File with TP migration information.
                          Format:
                           old_tp=abills_tp_id
    LOGIN2UID           - Convert login to uid for digit logins
    LIMIT               - Export only limiting account
    ADD_NAS             - Add nas servers from file. Fields defined via FILE_FIELDS=... option
    DB_HOST             -
    DB_USER             -
    DB_PASSWORD         -
    DB_CHARSET          -
    DB_NAME             -
    DB_PATH             - Path to DB file for carbon
    HTML                - Show export file in HTML FORMAT
    win2utf             - Convert info from win1251 to utf8
    help                - This help
[END]

}

#*******************************************************************
=head2 parse_arguments(@$argv) - Parse comand line arguments

=cut
#*******************************************************************
sub parse_arguments {
  my ($argv_) = @_;

  my %args = ();

  foreach my $line (@$argv_) {
    if ($line =~ /=/) {
      my ($k, $v) = split(/=/, $line, 2);
      $args{"$k"} = (defined($v)) ? $v : '';
    }
    else {
      $args{"$line"} = 1;
    }
  }
  return \%args;
}

#  `tos` tinyint(1) default NULL,
#  `do_with_tos` tinyint(1) default NULL,
#  `direction` tinyint(1) default NULL,
#  `fixed` tinyint(1) default NULL,
#  `fixed_cost` double(16,6) default NULL,
#  `activation_time` bigint(15) default NULL,
#  `total_time_limit` bigint(15) default NULL,
#  `month_time_limit` bigint(15) default NULL,
#  `week_time_limit` bigint(15) default NULL,
#  `day_time_limit` bigint(15) default NULL,
#  `total_traffic_limit` bigint(15) default NULL,
#  `month_traffic_limit` bigint(15) default NULL,
#  `week_traffic_limit` bigint(15) default NULL,
#  `day_traffic_limit` bigint(15) default NULL,
#  `total_money_limit` double(16,6) default NULL,
#  `month_money_limit` double(16,6) default NULL,
#  `week_money_limit` double(16,6) default NULL,
#  `day_money_limit` double(16,6) default NULL,
#  `login_time` varchar(254) default NULL,
#  `huntgroup_name` varchar(64) default NULL,
#
#  `port_limit` smallint(5) default NULL,
#  `session_timeout` bigint(15) default NULL,
#  `idle_timeout` bigint(15) default NULL,
#  `allowed_prefixes` varchar(64) default NULL,
#  `no_pass` tinyint(1) default NULL,
#  `no_acct` tinyint(1) default NULL,
#
#  `other_params` varchar(254) default NULL,
#  `total_time` bigint(15) NOT NULL default '0',
#  `total_traffic` bigint(15) NOT NULL default '0',
#  `total_money` double(16,6) NOT NULL default '0.000000',
#  `last_connection` date NOT NULL default '0000-00-00',
#  `framed_ip` varchar(16) NOT NULL default '',
#  `framed_mask` varchar(16) NOT NULL default '',
#  `callback_number` varchar(64) NOT NULL default '',
#  `speed` varchar(10) NOT NULL default '0',
#  PRIMARY KEY  (`uid`),
#  KEY `user` (`user`)

#**********************************************************
=head2 get_mikbill($attr) -  Export from Mikbill

  Arguments:
    $attr
      BLOCKED
      DELETED
      FREEZE

  Results:

=cut
#**********************************************************
sub get_mikbill {
  my ($attr) = @_;

  if (! defined($attr->{FREEZE})) {
    $attr->{FREEZE}=0;
  }

  my %fields = (
    'LOGIN'               => 'user',
    'PASSWORD'            => 'password',
    '1.EXPIRE'            => 'expired',
    '1.CREDIT'            => 'credit',
    '1.REGISTRATION'      => 'add_date',
    '4.DISABLE'           => 'blocked',
    '3.ADDRESS_FLAT'      => 'app',
    '3.ADDRESS_STREET'    => 'address',
    '3.ADDRESS_BUILD'     => 'houseid',
    '3.COMMENTS'          => 'prim',
    '3.CONTRACT_ID'       => 'numdogovor',
    '3.EMAIL'             => 'email',
    '3.FIO'               => 'fio',
    '5.PASPORT_GRANT'     => 'passportserie',
    '3.PHONE'             => 'phone',
    #'4.IP'                => 'framed_ip',
    '4.IP'                => 'static_ip',
    '4.NETMASK'           => 'framed_mask',
    '4.TP_NUM'            => 'gid',
    '4.CID'               => 'local_mac',
    '4.TP_NAME'           => 'tp_name',
    '4.MONTH_FEE'         => 'month_fee',
    '4.USER_CREDIT_LIMIT' => 'user_credit_limit',
    '4.SPEED'             => 'speed',
    '5.SUM'               => 'deposit',
    '3._DISTRICT'         => 'district',
    '3._CEL_PHONE'        => 'sms_tel',
    '3._MOB_TEL'          => 'mob_tel',
    '1.REDUCTION'         => 'reduction',
    '3.ENTRANCE'          => 'entrance',
    '3.FLOOR'             => 'floor',
    '1.DELETED'           => 'deleted',
    '1.DISABLE'           => 'disabled'
  );

  my %fields_rev = reverse(%fields);

  my $user_table = 'users';
  if ($attr->{BLOCKED}) {
    $user_table = 'usersblok';
  }
  elsif ($attr->{DELETED}) {
    $user_table = 'usersdel';
  }
  elsif ($attr->{FREEZE}) {
    $user_table = 'usersfreeze';
  }

  my $sql = "SELECT
  u.user,
  u.user,
  u.password,
  u.expired,
  u.credit,
  u.add_date,
  IF(u.blocked>0, 4, IF('$attr->{FREEZE}'<>'', 3, 0)) AS blocked,
  u.prim,
  u.numdogovor,
  u.email,
  u.fio,
  u.passportserie,
  u.phone,
  u.mob_tel,
  u.sms_tel,
  u.framed_mask,
  u.gid,
  u.deposit,
  u.local_mac,
  IF(u.real_ip=1, u.framed_ip, u.local_ip) AS static_ip,
  p.fixed_cost AS month_fee,
  p.packet AS tp_name,
  p.do_fixed_credit_summa AS user_credit_limit,
  u.fixed_cost AS reduction,
  IF(inetspeedlist.user_speed_in > 0, inetspeedlist.user_speed_in / 1024, '') AS speed,
  lanes_neighborhoods.neighborhoodname AS district,
  IF(u.app<>'', u.app, '') AS app,
  IF(addr.lane<>'', addr.lane, u.address) AS address,
  IF(h.house<>'', h.house, addr.house) AS houseid,
  addr.porches AS entrance,
  addr.floors AS floor,
  IF('$user_table'='usersdel', 1, 0) AS deleted,
  IF('$user_table'='usersblok', 1, 0) AS disabled

FROM $user_table u
  LEFT JOIN lanes_houses h ON ( u.houseid = h.houseid )
  LEFT JOIN lanes ON (h.laneid = lanes.laneid)
  LEFT JOIN lanes_neighborhoods ON (h.neighborhoodid = lanes_neighborhoods.neighborhoodid)
  LEFT JOIN packets p ON (u.gid = p.gid)
  LEFT JOIN inetspeedlist ON (u.user=inetspeedlist.username)
  LEFT JOIN usersadress addr ON (addr.user=u.user)

GROUP BY u.uid;
";

  if ($DEBUG > 4) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i <= $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], " . $fields_rev{"$query_fields->[$i]"} . " -> $row[$i] \n";
      }

      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }
    # elsif ($logins_hash{$LOGIN}{'3.COMMENTS'}) {
    #   $logins_hash{$LOGIN}{'3.COMMENTS'} =~ s/[\r\n]+/ /g;
    # }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }
  }

  undef($q);

  return \%logins_hash;
}

##**********************************************************
#=head2 get_mikbill_deleted()
#
#=cut
##**********************************************************
#sub get_mikbill_deleted {
#
#  my %fields = (
#    'LOGIN'               => 'user',
#    'PASSWORD'            => 'password',
#    '1.EXPIRE'            => 'expired',
#    '1.CREDIT'            => 'credit',
#    '1.REGISTRATION'      => 'add_date',
#    '4.DISABLE'           => 'blocked',
#    '3.ADDRESS_FLAT'      => 'app',
#    '3.ADDRESS_STREET'    => 'address',
#    '3.ADDRESS_BUILD'     => 'houseid',
#    '3.COMMENTS'          => 'prim',
#    '3.CONTRACT_ID'       => 'numdogovor',
#    '3.EMAIL'             => 'email',
#    '3.FIO'               => 'fio',
#    '5.PASPORT_GRANT'     => 'passportserie',
#    '3.PHONE'             => 'phone',
#    #'4.IP'                => 'framed_ip',
#    '4.IP'                => 'static_ip',
#    '4.NETMASK'           => 'framed_mask',
#    '4.TP_NUM'            => 'gid',
#    '4.CID'               => 'local_mac',
#    '4.TP_NAME'           => 'tp_name',
#    '4.MONTH_FEE'         => 'month_fee',
#    '4.USER_CREDIT_LIMIT' => 'user_credit_limit',
#    '4.SPEED'             => 'speed',
#    '5.SUM'               => 'deposit',
#    '3._DISTRICT'         => 'district',
#    '3._CEL_PHONE'        => 'sms_tel',
#    '3._MOB_TEL'          => 'mob_tel',
#    '1.REDUCTION'         => 'reduction',
#    '3.ENTRANCE'          => 'entrance',
#    '3.FLOOR'             => 'floor'
#  );
#
#  my %fields_rev = reverse(%fields);
#  #my $fields_list = "user, " . join(", \n", values(%fields));
#
#  my $sql = "SELECT
#  u.user,
#  u.user,
#  u.password,
#  u.expired,
#  u.credit,
#  u.add_date,
#  if(u.blocked>0, 4, 0) AS blocked,
#  u.prim,
#  u.numdogovor,
#  u.email,
#  u.fio,
#  u.passportserie,
#  u.phone,
#  u.mob_tel,
#  u.sms_tel,
#  u.framed_mask,
#  u.gid,
#  u.deposit,
#  u.local_mac,
#  IF(u.real_ip=1, u.framed_ip, u.local_ip) AS static_ip,
#  p.fixed_cost AS month_fee,
#  p.packet AS tp_name,
#  p.do_fixed_credit_summa AS user_credit_limit,
#  u.fixed_cost AS reduction,
#  IF(inetspeedlist.user_speed_in > 0, inetspeedlist.user_speed_in / 1024, '') AS speed,
#  lanes_neighborhoods.neighborhoodname AS district,
#  IF(u.app<>'', u.app, '') AS app,
#  IF(addr.lane<>'', addr.lane, u.address) AS address,
#  IF(h.house<>'', h.house, addr.house) AS houseid,
#  addr.porches AS entrance,
#  addr.floors AS floor
#
#FROM usersdel u
#  LEFT JOIN lanes_houses h ON ( u.houseid = h.houseid )
#  LEFT JOIN lanes ON (h.laneid = lanes.laneid)
#  LEFT JOIN lanes_neighborhoods ON (h.neighborhoodid = lanes_neighborhoods.neighborhoodid)
#  LEFT JOIN packets p ON (u.gid = p.gid)
#  LEFT JOIN inetspeedlist ON (u.user=inetspeedlist.username)
#  LEFT JOIN usersadress addr ON (addr.user=u.user)
#
#GROUP BY u.uid;";
#
#  #print $sql;
#  if ($DEBUG > 4) {
#    print $sql;
#    return 0;
#  }
#  elsif ($DEBUG > 0) {
#    print "$sql\n";
#  }
#  my DBI $q = $db->prepare($sql);
#  $q->execute();
#  my $query_fields = $q->{NAME};
#
#  #my $output = '';
#  my %logins_hash = ();
#
#  while (my @row = $q->fetchrow_array()) {
#    my $LOGIN = $row[0];
#
#    for (my $i = 1; $i <= $#row; $i++) {
#      if ($DEBUG > 3) {
#        print "$i, $query_fields->[$i], " . $fields_rev{"$query_fields->[$i]"} . " -> $row[$i] \n";
#      }
#
#      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
#    }
#
#    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
#      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
#    }
#    elsif ($logins_hash{$LOGIN}{'3.COMMENTS'}) {
#      $logins_hash{$LOGIN}{'3.COMMENTS'} =~ s/\n//g;
#    }
#
#    #Extended params
#    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
#      $logins_hash{$LOGIN}{$k} = $v;
#    }
#
#  }
#
#  undef($q);
#  return \%logins_hash;
#}
#
##**********************************************************
#=head2 get_mikbill_blocked() - GET bloked users from db mikbill
#
#=cut
##**********************************************************
#sub get_mikbill_blocked {
#
#  my %fields = (
#    'LOGIN'            => 'user',
#    'PASSWORD'         => 'password',
#    '1.EXPIRE'         => 'expired',
#    '1.CREDIT'         => 'credit',
#    '1.REGISTRATION'   => 'add_date',
#    '1.DISABLE'        => 'blocked',
#    '3.ADDRESS_FLAT'   => 'app',
#    '3.ADDRESS_STREET' => 'address',
#    '3.ADDRESS_BUILD'  => 'houseid',
#    '3.COMMENTS'       => 'prim',
#    '3.CONTRACT_ID'    => 'numdogovor',
#    '3.EMAIL'          => 'email',
#    '3.FIO'            => 'fio',
#    '5.PASPORT_GRANT'  => 'passportserie',
#    '3.PHONE'          => 'phone',
#    '4.IP'             => 'framed_ip',
#    '4.NETMASK'        => 'framed_mask',
#    '4.TP_NUM'         => 'gid',
#    '5.SUM'            => 'deposit',
#    '3._DISTRICT'      => 'lanes_neighborhoods.neighborhoodname AS ragion',
#    '3._CEL_PHONE'     => 'sms_tel',
#    '3._MOB_TEL'       => 'mob_tel',
#  );
#
#  my %fields_rev = reverse(%fields);
#  #my $fields_list = "user, " . join(", \n", values(%fields));
#
#  my $sql = "SELECT
#user,
#user,
#password,
#expired,
#credit,
#add_date,
#'1' as blocked,
#app,
#address,
#h.house as houseid,
#prim,
#numdogovor,
#email,
#fio,
#passportserie,
#phone,
#mob_tel,
#framed_ip,
#framed_mask,
#gid,
#deposit
#  FROM usersblok
#  LEFT JOIN lanes_houses h ON ( usersblok.houseid = h.houseid )";
#
#  #print $sql;
#  if ($DEBUG > 4) {
#    print $sql;
#    return 0;
#  }
#  elsif ($DEBUG > 0) {
#    print "$sql\n";
#  }
#
#  my DBI $q = $db->prepare($sql);
#  $q->execute();
#  my $query_fields = $q->{NAME};
#
#  #my $output = '';
#  my %logins_hash = ();
#
#  while (my @row = $q->fetchrow_array()) {
#    my $LOGIN = $row[0];
#
#    for (my $i = 1; $i <= $#row; $i++) {
#      if ($DEBUG > 3) {
#        print "$i, $query_fields->[$i], " . $fields_rev{"$query_fields->[$i]"} . " -> $row[$i] \n";
#      }
#
#      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
#    }
#
#    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
#      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
#    }
#    elsif ($logins_hash{$LOGIN}{'3.COMMENTS'}) {
#      $logins_hash{$LOGIN}{'3.COMMENTS'} =~ s/\n//g;
#    }
#
#    #Extended params
#    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
#      $logins_hash{$LOGIN}{$k} = $v;
#    }
#
#  }
#
#  undef($q);
#  return \%logins_hash;
#}

#**********************************************************
=head2 mikbill_pools() Export from Nodeny


=cut
#**********************************************************
sub mikbill_pools {

  my $sql = "INSERT INTO abills.ippools (name, netmask, ip, dns, gateway, vlan, comments, static, counts)
  SELECT sector, INET_ATON(mask), INET_ATON(subnet), dns_serv, INET_ATON(routers), vlanid,
    CONCAT(iface, '/', sectorid), 1, 253
   FROM sectors;";

  if ($debug > 3) {
    print $sql;
  }

  return 1;
}

#**********************************************************
=head2 mikbill_payments() - Export from mikbill payments

=cut
#**********************************************************
sub mikbill_payments {

  if ($debug > 1) {
    print "Plugin: mikbill_payments \n";
  }

  # GEt abills logins
  my $login2uid = login2uid();

  my DBI $db_abills = db_connect({ ABILLS_DB => 1 });
  my $sql = qq{
  SELECT
users.user AS login,
bugh_plategi_stat.date AS date,
bugh_plategi_type.deposit_action AS action,
bugh_plategi_stat.summa AS sum,
bugh_plategi_type.typename AS type,
bugh_plategi_stat.plategid AS id
FROM bugh_plategi_stat, bugh_plategi_type, users
WHERE
bugh_plategi_type.bughtypeid = bugh_plategi_stat.bughtypeid
AND users.uid = bugh_plategi_stat.uid
AND bugh_plategi_type.bughtypeid NOT in (1,2,9,20,21,22)
AND summa NOT in (0)
ORDER BY date;
  };

  my DBI $q = $db->prepare($sql);
  $q->execute();
  while (my $row = $q->fetchrow_hashref()) {
    my $uid = $login2uid->{$row->{login}}{UID} || 0;
    my $bill_id = $login2uid->{$row->{login}}{BILL_ID} || 0;

    if ($debug > 1) {
      print "LOGIN: $row->{login} UID: $uid BILL_ID: $bill_id SUM: $row->{sum} DATE: $row->{date}\n";
    }

    my $insert_query = "INSERT INTO payments (uid, bill_id, sum, date, ext_id)
     VALUES ($uid, $bill_id, '$row->{sum}', '$row->{date}', 'migrate: $row->{id}');";

    if ($debug > 1) {
      print "$insert_query\n";
    }
    $db_abills->do($insert_query);
  }

  return 1;
}


#**********************************************************
=head2 login2uid() - login to uid

=cut
#**********************************************************
sub login2uid {

  my %login2uid = ();
  # GEt abills logins
  my DBI $_db = db_connect({ ABILLS_DB => 1 });

  my $sql = "SELECT uid, id AS login, bill_id FROM users;";

  my DBI $q = $_db->prepare($sql);
  $q->execute();
  while (my $row = $q->fetchrow_hashref()) {
    $login2uid{$row->{login}}{UID} = $row->{uid};
    $login2uid{$row->{login}}{BILL_ID} = $row->{bill_id};
  }

  return \%login2uid;
}

#**********************************************************
=head2 get_nodeny() Export from Nodeny

  49.xx
  50.32

=cut
#**********************************************************
sub get_nodeny {

  $encryption_key = "hardpass3" if (!$encryption_key);

  my %fields = (
    '1.UID',             => 'u.id',
    'LOGIN'              => 'u.name',
    'PASSWORD'           => "AES_DECRYPT(passwd, \'$encryption_key\') AS password",
    '3.CONTRACT_DATE'    => 'DATE_FORMAT(FROM_UNIXTIME(contract_date), \'%Y-%m-%d\') AS activate',

    #  '1.EXPIRE'			=> 'expired',
    #  '1.COMPANY_ID'		=> '',
    '1.CREDIT'           => "(SELECT IF(SUM(cash) IS NULL, 0, SUM(cash)) FROM pays WHERE type='20' AND category='1000' AND mid=u.id ORDER BY time DESC LIMIT 1) AS credit",
    '1.CREDIT_DATE'      => "(SELECT IF(MAX(time) IS NULL, '', FROM_UNIXTIME(time)) FROM pays WHERE type='20' AND category='1000' AND mid=u.id ORDER BY time DESC LIMIT 1) AS credit_date",
    '1.GID'              => 'grp',
    '1.REDUCTION'        => 'discount',
    '3._STARTDAY'        => 'start_day',
    #  '1.REGISTRATION'		=> 'add_date',
    #  '1.DISABLE'			=> 'blocked',

    #  '3.ADDRESS_FLAT'		=> 'app',
    #  '3.ADDRESS_STREET'	=> 'address',
    #  '3.ADDRESS_BUILD'		=> 'houseid',
    '3.COMMENTS'         => 'u.comment',
    '3.CONTRACT_ID'      => 'contract',

    #  '3.EMAIL'				=> 'email',
    '3.FIO'              => 'fio',

    #  '5.PASPORT_GRANT'		=> 'passportserie',
    #  '3.PHONE'				=> 'mob_tel',

    #  '4.CID'				=> '',
    #  '4.FILTER_ID'		=> '',
    '4.IP'               => 'ip',
    #  '4.NETMASK'			=> 'framed_mask',
    #  '4.SIMULTANEONSLY'	=> 'simultaneous_use',
    #  '4.SPEED'			=> 'speed',
    #'4.TP_ID'            => 'paket',
    '4.TP_NAME'          => 'internet_tp.name AS internet_tp_name',
    #  '4.CALLBACK'			=> 'allow_callback',
    '5.SUM'              => "balance - (SELECT IF(SUM(cash) IS NULL, 0, SUM(cash)) FROM pays WHERE type='20' AND category='1000' AND mid=u.id ORDER BY time DESC LIMIT 1) AS sum",
    '3._SUM1'            => "balance",
    '3._SUM2'            => "(SELECT IF(SUM(cash) IS NULL, 0, SUM(cash)) FROM pays WHERE type='20' AND category='1000' AND mid=u.id ORDER BY time DESC LIMIT 1) AS _sum2",

    '3._EXTRA_SERVICE'   => 'u.srvs',
    '3._NEXT_PAKET'      => 'u.next_paket',
    '13.DATE'            => "if (next_paket>0, DATE_FORMAT(CURDATE()+INTERVAL 1 MONTH, '%Y-%m-01'), substring_index(pays.reason, ':', 1)) AS shedule_date",
    '13.TP_NAME'         => "if (next_paket>0, internet_tp_next.name, internet_tp_shedule.name) AS shedule_tp_name",
    #'11.TP_ID'           => 'paket3',
    '11.TP_NAME'         => 'iptv_tp.name AS iptv_tp_name',
    '11.CHANGE_TP_NAME', => 'next_paket3',
    #'11.CHANGE_TP_DATE'  => '',

    '3.SPEED_IN'  => "MAX(IF (v.dopfield_id = 1, v.field_value, '')) AS _speed_in",
    '3.SPEED_OUT'  => "MAX(IF (v.dopfield_id = 2, v.field_value, '')) AS _speed_out",
    '3._TCP_24'  => "MAX(IF (v.dopfield_id = 3, v.field_value, '')) AS _tcp_25",
    '4.CID'  => "MAX(IF (v.dopfield_id = 4, v.field_value, '')) AS mac",
    '3._FIELD_5'  => "MAX(IF (v.dopfield_id = 5, v.field_value, '')) AS field_5",
    '3.ADDRESS_STREET'  => "MAX(IF (v.dopfield_id = 5, s.name_street, ''))  AS address_street",
    '3.ADDRESS_BUILD'  => "MAX(IF (v.dopfield_id = 6, v.field_value, '')) AS address_build",
    '3._DOFIELD_7'  => "MAX(IF (v.dopfield_id = 7, v.field_value, '')) AS field_7",
    '3.ADDRESS_FLAT'  => "MAX(IF (v.dopfield_id = 8, v.field_value, '')) AS address_flat",
    '3.PHONE'  => "MAX(IF (v.dopfield_id = 9, v.field_value, '')) AS phone",
    '3._COMMENTS'  => "MAX(IF (v.dopfield_id = 10, v.field_value, '')) AS comments",

       '3._CHECK_LINE'  => "MAX(IF (v.dopfield_id = 14, v.field_value, '')) AS check_line",
       '3.PASPORT_NUM'  => "MAX(IF (v.dopfield_id = 15, v.field_value, '')) AS passport_num",
       '3.TAX_NUMBER'  => "MAX(IF (v.dopfield_id = 16, v.field_value, '')) AS inn",
       '3.REG_ADDRESS'  => "MAX(IF (v.dopfield_id = 17, v.field_value, '')) AS reg_address",
       '3.EMAIL'  => "MAX(IF (v.dopfield_id = 19, v.field_value, '')) AS email",
       '3._PON'  => "MAX(IF (v.dopfield_id = 20, v.field_value, '')) AS _pon",
       '3.ADDRESS_BLOCK'  => "MAX(IF (v.dopfield_id = 21, v.field_value, '')) AS address_block",
       '3._SKIP_SMS'  => "MAX(IF (v.dopfield_id = 22, v.field_value, '')) AS skip_sms",
       '3._PHONE2'  => "MAX(IF (v.dopfield_id = 23, v.field_value, '')) AS phone2",
       '3._IPTV_MAC'  => "MAX(IF (v.dopfield_id = 24, v.field_value, '')) as iptv_mac",
       '3.SWITCH_PASSWORD'  => "MAX(IF (v.dopfield_id = 25, v.field_value, '')) AS switch_password",
       '3._PASPORT_INFO'  => "MAX(IF (v.dopfield_id = 27, v.field_value, '')) AS _pasport_info",
       '3._ADS_POINT'  => "MAX(IF (v.dopfield_id = 28, v.field_value, '')) AS _ads_point",
       '3._NEXT_IPTV_TP'  => "MAX(IF (v.dopfield_id = 29, v.field_value, '')) AS _next_iptv_tp",
       '3._MEDIA_PLAYER'  => "MAX(IF (v.dopfield_id = 30, v.field_value, '')) AS _media_player",
       '3._ROUTER'  => "MAX(IF (v.dopfield_id = 31, v.field_value, '')) AS _router",
       '3._NEXT_INTERNET_TP'  => "MAX(IF (v.dopfield_id = 32, v.field_value, '')) AS next_internet_tp",
       '3._CONNECT_PRICE'  => "MAX(IF (v.dopfield_id = 33, v.field_value, '')) AS _connect_price",
       '3._BUILD_TYPE'  => "MAX(IF (v.dopfield_id = 34, v.field_value, ''))  AS _build_type",
       '3._USER_TYPE'  => "MAX(IF (v.dopfield_id = 35, v.field_value, '')) AS _user_type",
       '3._REFERRER'  => "MAX(IF (v.dopfield_id = 36, v.field_value, '')) AS _referrer",
       '3._IPTV_TP'  => "MAX(IF (v.dopfield_id = 37, v.field_value, '')) AS _iptv_tp",
       '3._SPEED_CONNECT'  => "MAX(IF (v.dopfield_id = 38, v.field_value, '')) AS _speed_connect",
       '3._CPE_UNITS'  => "MAX(IF (v.dopfield_id = 39, v.field_value, '')) AS _cpe_units",

    #  '5.DESCRIBE'			=> "'Migration'",
    #  '5.ER'				=> undef,
    #  '5.EXT_ID'			=> undef,

    #  '6.USERNAME'			=> 'email',
    #  '6.DOMAINS_SEL'		=> $email_domain_id || 0,
    #  '6.COMMENTS'			=> '',
    #  '6.MAILS_LIMIT'		=> 0,
    #  '6.BOX_SIZE'			=> 0,
    #  '6.ANTIVIRUS'		=> 0,
    #  '6.ANTISPAM'			=> 0,
    #  '6.DISABLE'          => 0,
    #  '6.EXPIRE'			=> undef,
    #  '6.PASSWORD'			=> 'email_pass',
  );

  my %fields2 = (
    '1.UID',             => 'u.id',
    'LOGIN'              => 'u.name',
    'PASSWORD'           => "AES_DECRYPT(passwd, \'$encryption_key\') AS password",
    '3.CONTRACT_DATE'    => 'DATE_FORMAT(FROM_UNIXTIME(contract_date), \'%Y-%m-%d\') AS activate',
    '1.GID'              => 'u.grp',
    '1.GID_NAME',        => 'user_grp.grp_name',
    '1.REDUCTION'        => 'u.discount',
    '3.COMMENTS'         => 'u.comment',
    '3.CONTRACT_ID'      => 'u.contract',
    '3.FIO'              => 'u.fio',
    '4.IP'               => 'u.ip',

    #  '4.NETMASK'			=> 'framed_mask',
    #  '4.SIMULTANEONSLY'	=> 'simultaneous_use',
    #  '4.SPEED'			=> 'speed',
    #'4.TP_ID'            => 'paket',
    '4.TP_NAME'          => 'internet_tp.name AS internet_tp_name',
    '5.SUM'              => 'u.balance',

    '3._EXTRA_SERVICE'   => 'u.srvs',
    '3._NEXT_PAKET'      => 'u.next_paket',
    '13.DATE'            => "if (next_paket>0, DATE_FORMAT(CURDATE()+INTERVAL 1 MONTH, '%Y-%m-01'), '') AS shedule_date",
    '13.TP_NAME'         => "if (next_paket>0, internet_tp_next.name, '') AS shedule_tp_name",

    '11.TP_ID'           => 'u.paket3',
    '11.TP_NAME'         => 'IF(iptv_tp.name IS null, iptv_megogo.name, iptv_tp.name) AS iptv_tp_name',
    '11.CHANGE_TP_NAME', => 'next_paket3',
    # '11.CHANGE_TP_DATE'  => '',
    # '11.SERVICE_ID'      => '',
    '11.SUBSCRIBE_ID'    => 'iptv_device.uuid',
    # '11.CID'             => '',
    # '11,PIN'             => '',
    # '11.STATUS'          => '',
    '11.PLAYLIST'        => 'iptv_device.url',

    #'3._DOPFIELD_1'  => "MAX(IF (v.dopfield_id = 1, k.field_name, ''))",
    # '3.SPEED_IN'         => "MAX(IF (v.dopfield_id = 1, v.field_value, '')) AS _speed_in",
    # '3.SPEED_OUT'        => "MAX(IF (v.dopfield_id = 2, v.field_value, '')) AS _speed_out",

    #'3.'  => "MAX(IF (v.dopfield_id = 4, k.field_name, ''))",
    '4.CID'            => "MAX(IF (v.dopfield_id = 4, v.field_value, '')) AS mac",
    '3.ADDRESS_STREET' => "MAX(IF (v.dopfield_id = 21, s.name_street, '')) AS address_street",
    '3._DISTRICT'      => "r.name AS district",
    '3.ADDRESS_BUILD'  => "MAX(IF (v.dopfield_id = 22, v.field_value, '')) AS address_build",
    '3.FLOOR'          => "MAX(IF (v.dopfield_id = 23, v.field_value, '')) AS floor",
    '3.ADDRESS_FLAT'   => "MAX(IF (v.dopfield_id = 24, v.field_value, '')) AS address_flat",
    '3._COMMENTS'      => "MAX(IF (v.dopfield_id = 26, v.field_value, '')) AS comments",
    '3.ENTRANCE'       => "MAX(IF (v.dopfield_id = 28, v.field_value, '')) AS entrance",

    '3.PASPORT_NUM'    => "MAX(IF (v.dopfield_id = 85, v.field_value, '')) AS passport_num",
    '3.PASPORT_GRANT'  => "MAX(IF (v.dopfield_id = 86, v.field_value, '')) AS passport_grant",
    '3.PASPORT_DATE'   => "MAX(IF (v.dopfield_id = 87, v.field_value, '')) AS passport_date",

    '3.REG_ADDRESS'    => "MAX(IF (v.dopfield_id = 88, v.field_value, '')) AS reg_address",
    '3.EMAIL'          => "MAX(IF (v.dopfield_id = 54, v.field_value, '')) AS email",
    '3.EMAIL'          => "MAX(IF (v.dopfield_id = 58, v.field_value, '')) AS email2",
    '3._SKIP_SMS'      => "MAX(IF (v.dopfield_id = 13, v.field_value, '')) AS skip_sms",
    '3._PHONE2'        => "MAX(IF (v.dopfield_id = 91, v.field_value, '')) AS _phone2",
    '3._PHONE3'        => "MAX(IF (v.dopfield_id = 14, v.field_value, '')) AS _phone3",
    '3.PHONE'          => "MAX(IF (v.dopfield_id = 25, v.field_value, '')) AS _phone",
    '3._CELL_PHONE'    => "MAX(IF (v.dopfield_id = 29, v.field_value, '')) AS _cell_phone",
    '3._ADDRESS_BLOCK' => "MAX(IF (v.dopfield_id = 27, v.field_value, '')) AS _block",
    '3._CPE_UNITS'     => "MAX(IF (v.dopfield_id = 20, v.field_value, '')) AS _cpe_units", #Serial
    '3._SEX'           => "MAX(IF (v.dopfield_id = 32, v.field_value, '')) AS _sex",
    '3.BIRTH_DATE'     => "MAX(IF (v.dopfield_id = 33, v.field_value, '')) AS _birthday",
    '3._USER_TYPE'     => "MAX(IF (v.dopfield_id = 35, v.field_value, '')) AS _user_type",

    # NAS
    '4.NAS_STATUS'     => "MAX(IF (v.dopfield_id = 36, v.field_value, '')) AS nas_status",
    '4.NAS_IP'         => "MAX(IF (v.dopfield_id = 37, v.field_value, '')) AS nas_ip",
    '4.NAS_MAC'        => "MAX(IF (v.dopfield_id = 38, v.field_value, '')) AS nas_mac",
    '4.NAS_NAME'       => "MAX(IF (v.dopfield_id = 79, v.field_value, '')) AS nas_name",

    '4.PORT'           => "MAX(IF (v.dopfield_id = 45, v.field_value, '')) AS port",
    #'4.SERVER_VLAN'    => "MAX(IF (v.dopfield_id = 59, v.field_value, '')) AS server_vlan",

    '4.SERVER_VLAN'    => "vlan2user.high_vlan AS server_vlan",
    '4.VLAN'           => "vlan2user.low_vlan AS vlan",
    '4.NAS_IP'         => "switch.ip AS nas_ip",
    '4.NAS_NAME'       => "switch.name AS nas_name",
    '4.NAS_DESCRIBE'   => "switch.bras AS bras",
    '4.NAS_MAC'        => "switch.mac AS nas_mac",
    '4.NAS_IDENTIFIER' => "switch.serial AS nas_identifier",

    '3._SN_ONU'        => "MAX(IF (v.dopfield_id = 62, v.field_value, '')) AS _sn_onu",
    '3._ONU_MAC'       => "MAX(IF (v.dopfield_id = 63, v.field_value, '')) AS _onu_mac",
  );

  my %fields_rev = reverse(%fields);
  my $fields_list = "u.name, " . join(", \n", values(%fields));

  foreach my $key (keys %fields_rev) {
    $key =~ /([a-z\_0-9]+)$/i;
    $fields_rev{$1}=$fields_rev{$key};
  }

  my $WHERE = q{};

  if ($argv->{ID} && $argv->{ID}=~/(\d+)\-(\d+)/) {
    $WHERE .= qq{ AND ( u.id >= $1 AND u.id <= $2 )  };
  }
  elsif($argv->{LOGIN}) {
    if ($argv->{LOGIN} =~ /,/) {
      my @logins = split(/,\s?/, $argv->{LOGIN});
      $WHERE .= " AND u.name IN ('". join("', '", @logins)  ."') ";
    }
    else {
      $WHERE .= qq{ AND u.name = '$argv->{LOGIN}' };
    }
  }
  elsif($argv->{NOT_GRP} && $argv->{NOT_GRP}) {
    $WHERE .= qq{ AND ( u.grp NOT IN ($argv->{NOT_GRP})  )  };
  }
  elsif($argv->{GROUP_ID}) {
    $WHERE .= qq{ AND ( u.grp IN ($argv->{GROUP_ID})  )  };
  }

  my $EXT_TABLES = q{};
  if ($fields{'4.SERVER_VLAN'}) {
    $EXT_TABLES .= "LEFT JOIN vlan2user ON (vlan2user.id=u.id) \n";
    $EXT_TABLES .= "LEFT JOIN p_switch_f switch ON (switch.switch=u.switch) \n";
    $EXT_TABLES .= "LEFT JOIN regions r ON (r.id=s.region)\n";
  }

  if ($fields{'11.TP_NAME'} && $fields{'11.TP_NAME'} =~ /iptv_megogo/) {
    $EXT_TABLES .= "LEFT JOIN hlstv_device iptv_device ON (iptv_device.mid=u.id) \n";
    $EXT_TABLES .= "LEFT JOIN usrvs iptv_megogo ON (iptv_megogo.mid=u.id AND iptv_megogo.date_k > unix_timestamp()) \n";
    #hlstv_packet
  }

  if ($fields{'13.TP_NAME'}) {
    $EXT_TABLES .= "LEFT JOIN pays ON (pays.mid=u.id AND pays.category = '431')
  LEFT JOIN plans2 internet_tp_shedule ON (internet_tp_shedule.id=substring_index(pays.reason, ':', -1))
  ";
  }

  my $sql = "SELECT $fields_list
    FROM users u
    LEFT OUTER JOIN dopvalues v ON (v.parent_id=u.id)
    LEFT JOIN dopfields k ON (k.id=v.dopfield_id)
    LEFT JOIN p_street s ON (s.street=v.field_value)
    LEFT JOIN plans2 internet_tp ON (internet_tp.id=u.paket)
    LEFT JOIN plans3 iptv_tp ON (iptv_tp.id=u.paket3)
    LEFT JOIN plans2 internet_tp_next ON (internet_tp_next.id=u.next_paket)
    LEFT JOIN user_grp ON (user_grp.grp_id=u.grp)
    $EXT_TABLES
    WHERE 
      (v.line_id = (SELECT max(line_id) FROM dopvalues WHERE v.parent_id=parent_id AND v.dopfield_id=dopfield_id GROUP BY parent_id )
       OR
       v.line_id IS NULL
      )
      $WHERE
    GROUP BY u.id
    ORDER BY u.id
  ";
  #`echo "$sql" >> /tmp/sql`;
  #print $sql;
  if ($DEBUG > 4) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }


  $db->do("SET SQL_BIG_SELECTS=1");

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  #my $output = '';
  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i < $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], " . ($fields_rev{$query_fields->[$i]} || q{}). " ->  ". ($row[$i]  || q{}) ."\n";
      }

      if ($query_fields->[$i] eq 'srvs') {
        my $services = get_dop_usluga($row[$i]);
        if ($services && $#{ $services } > -1) {
          $logins_hash{$LOGIN}{'9.TP_NAMES'} = join(',', @$services);
        }
      }
      elsif ($query_fields->[$i] eq 'address_street') {
        if ($row[$i] && $row[$i] =~ /(.+)\s?вул.\s?(.+)/) {
          my $city = $1;
          my $street = $2;
          $logins_hash{$LOGIN}{'3.ADDRESS_STREET'} = $street;
          $logins_hash{$LOGIN}{'3.CITY'} = $city;
        }
      }
      else {
        $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
      }
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }
    # elsif ($logins_hash{$LOGIN}{'3.COMMENTS'}) {
    #   $logins_hash{$LOGIN}{'3.COMMENTS'} =~ s/\n//g;
    # }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);
  return \%logins_hash;
}


#**********************************************************
=head2 get_dop_usluga($value) -

=cut
#**********************************************************
sub get_dop_usluga {
  my ($value) = @_;
  my @usluga = ();

  if (! $value) {
    return \@usluga;
  }

  my $value2 = log($value) / log(2) +1;
  my $count = int($value2);
  my @usluga_number = ();

  for (my $i=$count; $i>=1; $i--) {
    if ($value - 2 ** ($i-1) >= 0) {
      $usluga[$i] = 1;
      $value = $value - 2 ** ($i-1);
      push @usluga_number, $i;
    }
    else {
      $usluga[$i] = 0;
    }
  }

  shift @usluga;

  return \@usluga_number;
}

#**********************************************************
=head2 get_traffpro() - Export from Traffpro

=cut
#**********************************************************
sub get_traffpro {

  my %fields = (
    'LOGIN'            => 'login',
    'PASSWORD'         => 'key',
    '1.ACTIVATE'       => 'date',
    '1.GID'            => 'id_groups',
    '1.REDUCTION'      => 'discount',

    '3.ADDRESS_FLAT'   => 'apartment',
    '3.ADDRESS_STREET' => 'name',
    '3.ADDRESS_BUILD'  => 'house',
    '3.COMMENTS'       => 'comment',
    '3.CONTRACT_ID'    => 'num_contract',
    '3.EMAIL'          => 'email',
    '3.FIO'            => "surname", # surname - �������,  name - ��� , patronymic - ��������
    '3.PHONE'          => 'phone_mob',
    '3.PASPORT_DATE'   => 'passport_date',
    '3.PASPORT_GRANT'  => 'passport_create',
    '3.PASPORT_NUM'    => 'passport_namber',
    '3.CITY'           => 'name',

    '4.CID'            => 'addr_eth',
    '4.IP'             => 'addr_ip',
    '4.SPEED'          => 'speed',
    '4.TP_ID'          => 'traff_tarif',

    '5.SUM'            => 'traff_money_add',

  );

  my %fields_rev = reverse(%fields);
  #my $fields_list = "login, " . join(", \n", values(%fields));

  my $sql = "SELECT cl.login,
 				   pwd.key,
 				   cl.login,
 				   cl.date,
 				   cl.id_groups,
 				   tarif.discount,
 				   addr.apartment,
 				   str.name,
 				   addr.house,
 				   addr.comment,
 				   cont.num_contract,
 				   cont.email,
 				   cont.surname,  #	 cont.surname,	cont.name,  cont.patronymic,
 				   cont.phone_mob,
 				   cont.passport_date,
 				   cont.passport_create,
 				   cont.passport_namber,
 				   city.name,
 				   ca.addr_eth,
 				   ca.addr_ip,
 				   tarif.speed,
 				   ctcm.traff_tarif,
 				   ctcm.traff_money_add
			FROM `clients` AS cl
			LEFT JOIN clients_traff_check_money ctcm ON ( cl.id = ctcm.id )
			LEFT JOIN bus_tarif_plane tarif ON ( tarif.id = ctcm.traff_tarif )
			LEFT JOIN clients_addr ca ON ( ca.id = cl.id )
			LEFT JOIN contacts cont ON ( cont.id = cl.id )
			LEFT JOIN contacts_addr addr ON ( addr.id = cont.addr_registration )
			LEFT JOIN street str ON ( str.id = addr.street )
			LEFT JOIN city city ON ( city.city = str.city_id )
			LEFT JOIN clients_vpn pwd ON ( pwd.id = cl.id )";

  #print $sql;
  if ($DEBUG > 4) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  #my $output = '';
  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i <= $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], |" . $fields_rev{"$query_fields->[$i]"} . "| -> $row[$i] \n";
      }
      print "$i, $query_fields->[$i], |" . $fields_rev{"$query_fields->[$i]"} . "| -> $row[$i] \n";
      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i] || '';
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }
    # elsif ($logins_hash{$LOGIN}{'3.COMMENTS'}) {
    #   $logins_hash{$LOGIN}{'3.COMMENTS'} =~ s/\n//g;
    # }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);
  return \%logins_hash;
}

#**********************************************************
=head2 get_stargazer() Export from Stargazer

=cut
#**********************************************************
sub get_stargazer {

  my %fields = (
    'LOGIN'            => 'login',
    'PASSWORD'         => 'Password',
    '1.CREDIT'         => 'Credit',
    '1.GID'            => 'StgGroup',

    '3.ADDRESS_STREET' => 'Address',
    '3.COMMENTS'       => 'Note',
    '3.EMAIL'          => 'Email',
    '3.FIO'            => 'RealName',
    '3.PHONE'          => 'Phone',

    '4.IP'             => 'IP',
    '4.TP_ID'          => 'Tariff',

    '5.SUM'            => 'Cash',
  );

  my %fields_rev = reverse(%fields);
  my $fields_list = "login, " . join(", \n", values(%fields));
  my $users_table = ($argv->{UBILLING}) ? 'users' : 'tb_users';

  my $sql = "SELECT $fields_list FROM $users_table;";

  #print $sql;
  if ($DEBUG > 4) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }
  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i < $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], " . $fields_rev{"$query_fields->[$i]"} . " -> $row[$i] \n";
      }

      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }
    # elsif ($logins_hash{$LOGIN}{'3.COMMENTS'}) {
    #   $logins_hash{$LOGIN}{'3.COMMENTS'} =~ s/\n//g;
    # }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);
  return \%logins_hash;
}

#**********************************************************
=head2 get_stargazer_pg() - Export from Stargazer

=cut
#**********************************************************
sub get_stargazer_pg {

  my %fields = (
    'LOGIN'            => 'name',
    'PASSWORD'         => 'passwd',
    '1.CREDIT'         => 'credit',

    #  '1.GID'             => 'grp',
    '3.ADDRESS_STREET' => 'address',
    '3.COMMENTS'       => 'note',

    #  '3.EMAIL'           => 'email',
    '3.FIO'            => 'real_name',
    '3.PHONE'          => 'phone',
    '4.TP_ID'          => 'fk_tariff',
    '5.SUM'            => 'cash',
    '4.IP'             => 'ip'

    #tb_users.pk_user - ��������� ���� ������������
    #tb_users.last_cash_add - ����� ���������� ����������
    #tb_users.last_cash_add_time - ���� ���������� ����������

  );

  my %fields_rev = reverse(%fields);
  my $fields_list = "name, " . join(", \n", values(%fields));

  my $sql = "SELECT $fields_list
  FROM tb_users u
  LEFT JOIN tb_allowed_ip ip ON (ip.fk_user=u.pk_user)";

  #print $sql;
  if ($DEBUG > 4) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  #my $output = '';
  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i < $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], " . $fields_rev{"$query_fields->[$i]"} . " -> $row[$i] \n";
      }

      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }
    # elsif ($logins_hash{$LOGIN}{'3.COMMENTS'}) {
    #   $logins_hash{$LOGIN}{'3.COMMENTS'} =~ s/\n//g;
    # }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);
  return \%logins_hash;
}

#**********************************************************
=head2 get_bbilling() Export from  nousaibot billing

=cut
#**********************************************************
sub get_bbilling {

  my %fields = (
    'LOGIN'             => 'CardNumber',
    'PASSWORD'          => 'PIN',

    '3._serialnumber'   => 'SerialNumber',
    '3._tariffplanname' => 'TariffPlanName',
    '3._subscribername' => 'SubscriberName',

    '4.TP_ID'           => 'TariffPlanNameID',

    '5.SUM'             => 'Saldo',

  );

  my %fields_rev = reverse(%fields);
  my $fields_list = "CardNumber, " . join(", \n", values(%fields));

  my $sql = "SELECT $fields_list FROM data";

  #print $sql;
  if ($DEBUG > 4) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  #my $output      = '';
  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i < $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], " . $fields_rev{"$query_fields->[$i]"} . " -> $row[$i] \n";
      }

      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }
    # elsif ($logins_hash{$LOGIN}{'3.COMMENTS'}) {
    #   $logins_hash{$LOGIN}{'3.COMMENTS'} =~ s/\n//g;
    # }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);
  return \%logins_hash;
}

#**********************************************************
=head2 get_easyhotspot() Export easyhotspot

=cut
#**********************************************************
sub get_easyhotspot {

  my %fields = (
    'LOGIN'    => 'username',
    'PASSWORD' => 'password',
    '4.TP_ID'  => 'id',
  );

  my %fields_rev = reverse(%fields);

  #my $fields_list = "username, password ". join(", \n", values(%fields));

  my $sql = "SELECT  v.username,
 					v.password,
 					v.username,
 				   	v.password,
 				   	b.id
			FROM `voucher` AS v
			LEFT JOIN billingplan b ON ( b.name = v.billingplan )";

  #print $sql;
  if ($DEBUG > 4) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i <= $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], |" . $fields_rev{"$query_fields->[$i]"} . "| -> $row[$i] \n";
      }
      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i] || '';
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }
    # elsif ($logins_hash{$LOGIN}{'3.COMMENTS'}) {
    #   $logins_hash{$LOGIN}{'3.COMMENTS'} =~ s/\n//g;
    # }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);
  return \%logins_hash;
}

#**********************************************************
=head2 get_lms() Export from lms

   http://www.lms.org.pl/doc_en/devel-db.html
   http://www.lms.org.pl

=cut
#**********************************************************
sub get_lms {

  my %fields = (
    'LOGIN'            => 'id',
    'PASSWORD'         => 'pin',

    '1.GID'            => 'customergroupid',
    '1.REGISTRATION'   => 'FROM_UNIXTIME(c.creationdate)',
    '1.DISABLE'        => 'deleted',

    '3.ADDRESS_STREET' => 'if(c.address!=\'\', c.address, \'\')',

    '3.COMMENTS'       => 'if(c.message!=\'\', c.message, \'\')',
    '3.EMAIL'          => 'if(c.email!=\'\', c.email, \'\')',
    '3.FIO'            => 'if(c.lastname!=\'\', c.lastname, \'\')',

    '3.PHONE'          => 'if(c.phone1!=\'\', c.phone1, \'\')',
    '3.ZIP'            => 'if(c.zip!=\'\', c.zip, \'\')',
    '3.CITY '          => 'if(c.city!=\'\', c.city, \'\')',
    '4.TP_ID'          => 'tariffplan',
    '5.SUM'            => 'if((SELECT SUM(value) FROM cash WHERE customerid=c.id)!=\'\', (SELECT SUM(value) FROM cash WHERE customerid=c.id), \'\')',

  );

  my %fields_rev = reverse(%fields);

  #my $fields_list = "id, pin". join(", \n", values(%fields));

  my $sql = "select 	c.id as id,
 					c.id as id,
 					c.pin as pin,
 					if(ca.customergroupid!=\'\', ca.customergroupid, \'\') as customergroupid,
 					FROM_UNIXTIME(c.creationdate),
 					c.deleted as deleted,
 					if(c.address!=\'\', c.address, \'\'),
 					if(c.message!=\'\', c.message, \'\'),
 					if(c.email!=\'\', c.email, \'\'),
 					if(c.lastname!=\'\', c.lastname, \'\'),
 					if(c.phone1!=\'\', c.phone1, \'\'),
 					if(c.zip!=\'\', c.zip, \'\'),
 					if(c.city!=\'\', c.city, \'\'),
 					if(a.tariffid!=\'\', a.tariffid, \'\')  as tariffplan,
 					if((SELECT SUM(value) FROM cash WHERE customerid=c.id)!=\'\', (SELECT SUM(value) FROM cash WHERE customerid=c.id), \'\')

  FROM (customers c)
  LEFT JOIN assignments a ON (a.customerid=c.id)
  LEFT JOIN customerassignments ca ON (ca.customerid=c.id)

  GROUP BY c.id
  ORDER BY 1";

  if ($DEBUG > 5) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 1; $i <= $#row; $i++) {
      if ($DEBUG > 3) {
        print "$i, $query_fields->[$i], " . $fields_rev{"$query_fields->[$i]"} . " -> $row[$i] \n";
      }

      $logins_hash{$LOGIN}{ $fields_rev{ $query_fields->[$i] } } = $row[$i];
    }

    if ((!$logins_hash{$LOGIN}{'5.SUM'})) {
      $logins_hash{$LOGIN}{'5.SUM'} = '0.00';
    }

    if ($logins_hash{$LOGIN}{'6.USERNAME'} && $logins_hash{$LOGIN}{'6.USERNAME'} =~ /(\S+)\@/) {
      $logins_hash{$LOGIN}{'6.USERNAME'} = $1;
    }
    # elsif ($logins_hash{$LOGIN}{'3.COMMENTS'}) {
    #   $logins_hash{$LOGIN}{'3.COMMENTS'} =~ s/\n//g;
    # }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);
  return \%logins_hash;
}

#**********************************************************
=head2 get_lms_nodes() Export lms nodes

   http://www.lms.org.pl/doc_en/devel-db.html
   http://www.lms.org.pl

=cut
#**********************************************************
sub get_lms_nodes {

  my $sql = "select ownerid, inet_ntoa(ipaddr), mac from nodes";

  if ($DEBUG > 0) {
    print "$sql\n";
  }
  my DBI $q = $db->prepare($sql);
  $q->execute();

  #my $query_fields = $q->{NAME};

  while (my @row = $q->fetchrow_array()) {
    print "$row[0]\t$row[1]\t$row[2]\n";
  }

  exit();
}

#**********************************************************
=head2 get_odbc()

=cut
#**********************************************************
sub get_odbc {

  #, , , , , , connspeed__id

  my %fields = (
    'LOGIN'         => 'user__login',
    'PASSWORD'      => 'user__pass',

    #  '1.ACTIVATE'     => 'activated',
    #  '1.EXPIRE' 	     => 'expired',
    #  '1.COMPANY_ID'   => '',
    '1.CREDIT_DATE' => 'user__createdt',
    '1.GID'         => 'group__id',

    #  '1.REDUCTION'    => '',
    #'1.REGISTRATION' => 'DATE_FORMAT(FROM_UNIXTIME(reg_date), \'%Y-%m-%d\')',
    '4.DISABLE'     => 'user_state__id',
    '1.DELETED'     => 'user__deletedt',

    #  '3.ADDRESS_FLAT'   => '',
    #  '3.ADDRESS_STREET' => 'actual_address',
    #  '3.ADDRESS_BUILD'  => '',
    #  '3.COMMENTS'       => '',
    #  '3.CONTRACT_ID' 	 => '',
    #'3.EMAIL' => 'email',
    #'3.FIO'   => 'full_name',
    #  '3.PHONE'          => 'phone',
    #  '4.CID'            => '',
    #  '4.FILTER_ID'      => '',
    '4.IP'          => 'user__framed_ip_address',

    #  '4.NETMASK'        => '\'255.255.255.255\'',
    #  '4.SIMULTANEONSLY' => 'simultaneous_use',
    #  '4.SPEED'          => 'speed',
    '4.TP_ID'       => 'tariff__id',

    #  '4.CALLBACK'       => 'allow_callback',

    #'5.SUM' => 'bill',

    #  '5.DESCRIBE' 	     => "'Migration'",
    #  '5.ER'             => undef,
    #  '5.EXT_ID'         => undef,

    #  '6.USERNAME'       => 'email',
    #  '6.DOMAINS_SEL'     => $email_domain_id || 0,
    #  '6.COMMENTS'        => '',
    #  '6.MAILS_LIMIT'	    => 0,
    #  '6.BOX_SIZE'	      => 0,
    #  '6.ANTIVIRUS'	      => 0,
    #  '6.ANTISPAM'	      => 0,
    #  '6.DISABLE'	        => 0,
    #  '6.EXPIRE'	        => undef,
    #  '6.PASSWORD'	      => 'email_pass',
  );

  my %fields_rev = reverse(%fields);
  my $fields_list = join(",\n ", values(%fields));

  my $sql = "SELECT $fields_list FROM users";
  print "$sql\n" if ($DEBUG > 1);

  my DBI $q = $db->prepare($sql);
  $q->execute();
  #my $query_fields = $q->{NAME};

  #my $output = '';
  my %logins_hash = ();

  while (my $row = $q->fetchrow_hashref()) {
    my $LOGIN = $row->{ $fields{LOGIN} };
    if ($DEBUG > 3) {
      print "$LOGIN ============= \n";
    }

    # Field name
    while (my ($k, $v) = each %$row) {
      if ($DEBUG > 3) {
        print "$k, $fields_rev{$k} -> " . ((defined($v)) ? $v : '') . "\n";
      }

      $logins_hash{$LOGIN}{ $fields_rev{$k} } = $v;
    }

    if ($logins_hash{$LOGIN}{'1.CREDIT'} && $logins_hash{$LOGIN}{'1.CREDIT'} =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/) {
      $logins_hash{$LOGIN}{'1.CREDIT'} = $1;
    }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }

  }

  undef($q);
  return \%logins_hash;

}

#**********************************************************
=head2 get_carbon4() - Import from Carbon 4

=cut
#**********************************************************
sub get_carbon4 {
  my %fields = (
    'LOGIN'            => 'LOGIN',
    'PASSWORD'         => 'PSW',
    '1.ACTIVATE'       => 'ACTIVATE_DATE',
    '1.GID'            => 'PARID',
    '1.DISABLE'        => 'DISABLED',

    #    '1.REDUCTION' => 'discount',

    '3.ADDRESS_FLAT'   => 'A_HOME_NUMBER',
    '3.ADDRESS_STREET' => 'STREET',
    '3.ADDRESS_BUILD'  => 'S_NUMBER',

    #    '3.COMMENTS'       => 'comment',
    #    '3.CONTRACT_ID'    => 'num_contract',
    '3.EMAIL'          => 'EMAIL',
    '3.FIO'            => 'IDENTIFY',
    '3.PHONE'          => 'PHONE',
    '3.PASPORT_DATE'   => 'PASPORT_DATE',
    '3.PASPORT_GRANT'  => 'PASPORT_GRANT',
    '3.PASPORT_NUM'    => 'PASPORT_NUM',
    '3.CITY'           => 'CITY',

    '4.CID'            => 'MAC',
    '4.IP'             => 'IP',
    '4.SPEED'          => 'LIMIT',
    '4.TP_ID'          => 'TARIFF_NO',

    '5.SUM'            => 'OSTATOK',

  );

  #  my %fields_rev = reverse(%fields);
  #  my $fields_list = "login, " . join(", \n", values(%fields));

  my %attribute_type_id_for = (
    PHONE        => 1,
    PASPORT_NUM  => 13,

    #    PASPORT_SER => 15,
    PASPORT_BY   => 16,
    PASPORT_DATE => 17,
  );

  my $sql = "SELECT USERS.LOGIN,
                    USERS.PSW,
                    USERS.ENABLED AS ENABLED,
                    0 AS DISABLED,
                    USERS.ID,
                    USERS.ACTIVATE_DATE,
                    USERS.PARID,
                    USERS.A_HOME_NUMBER,
                    HOMES.CITY,
                    HOMES.STREET,
                    HOMES.S_NUMBER,
                    USERS.EMAIL,
                    USERS.IDENTIFY,
                    USERS.MAC,
                    USERS.IP,
                    USERS.OSTATOK,
                    (SELECT ATTRIBUTE_VALUE FROM ATTRIBUTE_VALUES WHERE USER_ID=USERS.ID AND ATTRIBUTE_ID=$attribute_type_id_for{PHONE}) AS PHONE,
    (SELECT ATTRIBUTE_VALUE FROM ATTRIBUTE_VALUES WHERE USER_ID=USERS.ID AND ATTRIBUTE_ID=$attribute_type_id_for{PASPORT_NUM}) AS PASPORT_NUM,
    (SELECT ATTRIBUTE_VALUE FROM ATTRIBUTE_VALUES WHERE USER_ID=USERS.ID AND ATTRIBUTE_ID=$attribute_type_id_for{PASPORT_BY}) AS PASPORT_GRANT,
    (SELECT ATTRIBUTE_VALUE FROM ATTRIBUTE_VALUES WHERE USER_ID=USERS.ID AND ATTRIBUTE_ID=$attribute_type_id_for{PASPORT_DATE}) AS PASPORT_DATE

     FROM USERS
     LEFT JOIN HOMES ON (HOMES.ID = USERS.HOME_ID);
          ";

  #print $sql;
  if ($DEBUG > 4) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }
  my DBI::st $q = $db->prepare($sql);
  $q->execute();

  my $users_list = $q->fetchall_hashref('ID');

  my %without_logins = ();
  foreach my $user (values %{$users_list}) {

    # Translating carbon 'ENABLED' to abills 'disabled' attr
    $user->{DISABLED} = !$user->{ENABLED};

    if (!$user->{LOGIN}) {
      $without_logins{ $user->{ID} } = $user;

      #      print "!!! User $user->{ID}( $user->{IDENTIFY}  ) don't have login. \n Will not be included in results \n";
      delete $users_list->{ $user->{ID} };
    }
  }

  # DB charset is CP1251, and we are working in UTF8
  # so need to convert some of columns got from DB

  eval {require Encode;};

  if ($@) {
    print "Please install 'Encode' perl module\n";

    #    print "Manual: http://abills.net.ua/wiki/doku.php/abills:docs:manual:soft:perl_odbc \n";
    exit;
  }
  Encode->import();

  my $convert_in_hashref_sub = sub {

    my ($list, $columns_to_convert) = @_;
    foreach my $row (@{$list}) {
      foreach my $column_name (@{$columns_to_convert}) {
        $row->{$column_name} = Encode::encode('utf8', Encode::decode('cp1251', $row->{$column_name}));
      }
    }

    return $list;
  };

  my @columns_can_contain_cyrillic_values = qw/IDENTIFY PASPORT_GRANT PSW/;
  $users_list = &{$convert_in_hashref_sub}([ values %{$users_list} ], \@columns_can_contain_cyrillic_values);

  foreach my $user (@{$users_list}) {
    if (exists $without_logins{ $user->{PARID} }) {

      #      print "$user->{IDENTIFY} is probably a group \n";
    }
  }

  # Form result
  my %result_hash;
  foreach my $user_row (@{$users_list}) {

    my $login = $user_row->{LOGIN};
    my $password = $user_row->{PSW} || $DEFAULT_PASSWORD;
    if ($password =~ /^\0+$/){
      $password = $DEFAULT_PASSWORD;
    }

    next if ($user_row->{LOGIN} =~ /\d+\-\d+/);

    delete $user_row->{LOGIN};
    delete $user_row->{PSW};

    my %attributes_hash;

    $attributes_hash{LOGIN} = $login;
    $attributes_hash{PASSWORD} = $password;

    # Saving all other attributes
    foreach my $attribute_name (keys %fields) {
      my $attr_value = $user_row->{ $fields{$attribute_name} };
      next if (!defined($attr_value) || $attr_value eq '' || $attr_value !~ /[0-9a-zA-Zа-яА-Я_]+/);
      $attributes_hash{$attribute_name}=$attr_value;
    }

    $result_hash{$login} = \%attributes_hash;
  }
  return \%result_hash;
}

#**********************************************************
=head2 convert_date() - Converts date from different formats to YYYY-MM-DD

=cut
#**********************************************************
sub convert_date{
  my ($date) = @_;
  my $year, my $month, my $day;
  if($date =~ /^(\d{4})-(\d{2})-(\d{2})$/){
    ($year, $month, $day) = ($1, $2, $3);
  }
  if($date =~ /^(\d{2})\.(\d{2})\.(\d{4})$/){
    ($year, $month, $day) = ($3, $2, $1);
  }
  if($date =~ /^(\d{2})\.(\d{2})\.(\d{2})$/){
    ($year, $month, $day) = ($3, $2, $1);
    if ($year > 30){ #TODO
      $year += 1900;
    }else{
      $year += 2000;
    }
  }
  if(defined($year)){
    return $year."-".$month."-".$day;
  }
  else{
    return $date;
  }

}

#**********************************************************
=head2 get_carbon5() - Import from Carbon 5

=cut
#**********************************************************
sub get_carbon5 {
  my %fields = (
    'LOGIN'            => 'LOGIN',
    'PASSWORD'         => 'GEN_PWD',
    '1.ACTIVATE'       => 'ACTIVATE',
    '3.CONTRACT_ID'    => 'CONTRACT_NUMBER',
    '3.CONTRACT_DATE'  => 'CONTRACT_DATE',
    '3.FIO'            => 'NAME',
    '3._COUNTRY'       => 'COUNTRY',
    '3.CITY'           => 'CITY',
    '3._REGION'        => 'REGION',
    '3.ADDRESS_STREET' => 'STREET',
    '3.ADDRESS_BUILD'  => 'S_NUMBER',

    '3.PHONE'          => 'SMS',
    '3.EMAIL'          => 'EMAIL',
    '4.TP_NUM'         => 'TP_NUM',
    '5.SUM'            => 'OSTATOK',
    '3.PASPORT_DATE'   => 'PASPORT_DATE',
    '3.PASPORT_GRANT'  => 'PASPORT_GRANT',
    '3.PASPORT_NUM'    => 'PASPORT_NUM',
    '4.LOGIN'          => 'LOGIN',
    '4.PASSWORD'       => 'GEN_PWD',
    '4.IP'             => 'IP',
    '11.IPTV_LOGIN'    => 'IPTV_LOGIN',
    '11.IPTV_PASSWORD' => 'IPTV_PASSWORD',
    '1.REDUCTION'      => 'LOYALTY',

    '12.TP_ID'         => 'TP_ID',
    '12.TP_NAME'       => 'TP_NAME',
    '12.TP_SUM'        => 'TP_SUM',


    #'1.GID'            => 'PARID',
    #'1.DISABLE'        => 'DISABLED',


  );

  #  my %fields_rev = reverse(%fields);
  #  my $fields_list = "login, " . join(", \n", values(%fields));

  my %attribute_type_id_for = (
    PHONE        => 1,
    PASPORT_NUM  => 13,

    #    PASPORT_SER => 15,
    PASPORT_BY   => 16,
    PASPORT_DATE => 17,
  );

  my $sql = 'SELECT A.ID,
            (SELECT FIRST(1) U.LOGIN FROM USERS AS U WHERE U.ABONENT_ID=A.ID AND U.IP IS NOT NULL),
            (SELECT FIRST(1) U.GEN_PWD FROM USERS AS U WHERE U.ABONENT_ID=A.ID AND U.IP IS NOT NULL),
            A.CONTRACT_NUMBER,
            CAST(A.CREATE_DATE as date) AS CONTRACT_DATE,
            A.NAME,
            H.COUNTRY,H.CITY,H.REGION,H.STREET,H.S_NUMBER,H.S_LITER,
            A.SMS,
            A.EMAIL,
            round((aa.ostatok+aa.debit-aa.credit) / cast((10000000000) as numeric(18,5)), 2) AS OSTATOK,
            T.ID AS TP_NUM,
            L.NAME AS LOYALTY,
            (select AV.ATTRIBUTE_VALUE as "PASPORT_DATE"
            from ATTRIBUTE_VALUES AV
            where AV.ATTRIBUTE_ID = 17
            and AV.ABONENT_ID = A.ID),
            (select AV.ATTRIBUTE_VALUE as "PASPORT_GRANT"
            from ATTRIBUTE_VALUES AV
            where AV.ATTRIBUTE_ID = 16
            and AV.ABONENT_ID = A.ID),
            (select AV.ATTRIBUTE_VALUE
            from ATTRIBUTE_VALUES AV
            where AV.ATTRIBUTE_ID = 14
            and AV.ABONENT_ID = A.ID)||
            (select AV.ATTRIBUTE_VALUE
            from ATTRIBUTE_VALUES AV
            where AV.ATTRIBUTE_ID = 13
            and AV.ABONENT_ID = A.ID) as "PASPORT_NUM",
            (SELECT FIRST(1) UF_IP2STRING(IP) FROM USERS AS U WHERE U.ABONENT_ID=A.ID AND U.IP IS NOT NULL) AS IP,
            (SELECT NAME FROM IP_PULL WHERE PULL_ID = (
             SELECT FIRST(1) PULL_ID FROM USERS AS U WHERE U.ABONENT_ID=A.ID AND U.IP IS NOT NULL)) AS IP_PULL,
            (SELECT MIN(NEXT_DATE) FROM USERS_USLUGA WHERE ABONENT_ID=A.ID AND NEXT_DATE > current_timestamp)-30 AS ACTIVATE,--
            (SELECT FIRST(1) U.LOGIN FROM USERS AS U WHERE U.ABONENT_ID=A.ID AND U.IP IS NULL) AS IPTV_LOGIN,
            (SELECT FIRST(1) U.GEN_PWD FROM USERS AS U WHERE U.ABONENT_ID=A.ID AND U.IP IS NULL) AS IPTV_PASSWORD,

            (SELECT FIRST(1) u.ID FROM USLUGA u
            LEFT JOIN USERS_USLUGA uu ON uu.ABONENT_ID = a.ID
            LEFT JOIN TARIF_USERS_USLUGA tuu ON u.ID = tuu.USLUGA_ID
            LEFT JOIN TARIF t ON tuu.TARIF_ID = t.ID
            WHERE u.ID = uu.USLUGA_ID AND
            uu.DELETED = 0 AND UU.ENABLED = 1 AND
            t.ID IS NULL) AS TP_ID,
            (SELECT FIRST(1) u.NAME FROM USLUGA u
            LEFT JOIN USERS_USLUGA uu ON uu.ABONENT_ID = a.ID
            LEFT JOIN TARIF_USERS_USLUGA tuu ON u.ID = tuu.USLUGA_ID
            LEFT JOIN TARIF t ON tuu.TARIF_ID = t.ID
            WHERE u.ID = uu.USLUGA_ID AND
            uu.DELETED = 0 AND UU.ENABLED = 1 AND
            t.ID IS NULL) AS TP_NAME,
            (SELECT FIRST(1) round((u.SUMMA) / cast((10000000000) as numeric(18,5)), 2) FROM USLUGA u
            LEFT JOIN USERS_USLUGA uu ON uu.ABONENT_ID = a.ID
            LEFT JOIN TARIF_USERS_USLUGA tuu ON u.ID = tuu.USLUGA_ID
            LEFT JOIN TARIF t ON tuu.TARIF_ID = t.ID
            WHERE u.ID = uu.USLUGA_ID AND
            uu.DELETED = 0 AND UU.ENABLED = 1 AND
            t.ID IS NULL) AS TP_SUM

            FROM ABONENTS AS A
            LEFT JOIN HOMES AS H on H.ID = A.HOME_ID
            left join ADMIN_ACCOUNTS AA on AA.ID=A.ACCOUNT_ID
            left join TARIF T on T.ID = A.TARIF_ID
            LEFT JOIN LOYALTYS L on L.ID = A.LOYALTY_ID';

  #print $sql;
  if ($DEBUG > 4) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }
  my DBI::st $q = $db->prepare($sql);
  $q->execute();

  my $users_list = $q->fetchall_hashref('ID');

  my %without_logins = ();
  foreach my $user (values %{$users_list}) {

    # Translating carbon 'ENABLED' to abills 'disabled' attr
    $user->{DISABLED} = !$user->{ENABLED};

    if (!$user->{LOGIN}) {
      $without_logins{ $user->{ID} } = $user;

      #      print "!!! User $user->{ID}( $user->{IDENTIFY}  ) don't have login. \n Will not be included in results \n";
      delete $users_list->{ $user->{ID} };
    }
  }

  $users_list = [values %{$users_list}];

  #foreach my $user (@{$users_list}) {
  #  if (exists $without_logins{ $user->{PARID} }) {
  #
  #    #      print "$user->{IDENTIFY} is probably a group \n";
  #  }
  #}

  # Form result
  my %result_hash;
  foreach my $user_row (@{$users_list}) {

    my $login = $user_row->{LOGIN};
    my $password = $user_row->{PSW} || $DEFAULT_PASSWORD;
    if ($password =~ /^\0+$/){
      $password = $DEFAULT_PASSWORD;
    }

    if (($user_row->{LOGIN} =~ /\d+\-\d+/)||($user_row->{LOGIN} =~ /^\0/)){
      next;
    }

    delete $user_row->{LOGIN};
    delete $user_row->{PSW};

    my %attributes_hash;

    $attributes_hash{LOGIN} = $login;
    $attributes_hash{PASSWORD} = $password;

    # Saving all other attributes
    foreach my $attribute_name (keys %fields) {
      my $attr_value = $user_row->{ $fields{$attribute_name} };
      if (!defined($attr_value) || $attr_value eq '' || $attr_value !~ /[0-9a-zA-Zа-яА-Я_]+/ || $attr_value =~ /^\0/){
        next;
      }
      if($attribute_name eq '3.PASPORT_DATE') {$attr_value=convert_date($attr_value);}
      $attributes_hash{$attribute_name}=$attr_value;
    }

    $result_hash{$login} = \%attributes_hash;
  }
  return \%result_hash;
}

#**********************************************************
=head2 sync_deposit($update_list) - Syncing deposits from file

  Arguments:
    $update_list - array of hash_ref
    {
      LOGIN   - user id,
      NEW_SUM - new balance value
    }

=cut
#**********************************************************
sub sync_deposit {
  my ($update_list) = @_;

  foreach my $user_hash (values %{$update_list}) {
    my $login = $user_hash->{LOGIN};
    my $new_sum = $user_hash->{NEW_SUM};

    my $sql = "UPDATE bills SET deposit= ? WHERE uid=(SELECT uid FROM users WHERE id= ? )";

    my DBI $q = $db->prepare($sql);

    $q->execute($new_sum, $login);

    print 'Changed ' . $login . ' deposit to ' . $new_sum . "\n";
  }

  return 1;
}


#**********************************************************
=head2 get_nika($update_list) - Get user information from nika

  Arguments:

=cut
#**********************************************************
sub get_nika {

  print "Nika stystem SQL examples\n";

  my $sql = qq{
TRUNCATE TABLE abills.groups;
TRUNCATE TABLE abills.users_contacts;
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE abills.users;
TRUNCATE TABLE abills.bills;
TRUNCATE TABLE abills.docs_invoice2payments;
TRUNCATE TABLE abills.payments;
TRUNCATE TABLE abills.fees;
TRUNCATE TABLE abills.docs_invoice_orders;
TRUNCATE TABLE abills.docs_invoices;
TRUNCATE TABLE abills.docs_receipt_orders;
TRUNCATE TABLE abills.docs_receipts;
TRUNCATE TABLE abills.maps_points;
TRUNCATE TABLE abills.builds;
TRUNCATE TABLE abills.admin_actions;
TRUNCATE TABLE abills.admin_system_actions;
SET FOREIGN_KEY_CHECKS = 1;
TRUNCATE TABLE abills.internet_main;
TRUNCATE TABLE abills.districts;
TRUNCATE TABLE abills.streets;
TRUNCATE TABLE abills.users_pi;
TRUNCATE TABLE abills.abon_tariffs;
TRUNCATE TABLE abills.abon_user_list;

SET SQL_MODE=NO_ENGINE_SUBSTITUTION;

#GROUPS
INSERT INTO abills.groups (gid, name, descr)   SELECT     id,     name,     comment   FROM     nika_system.groups;
#Users
INSERT INTO abills.users (uid, id,gid, disable, bill_id, registration, password)
  SELECT id, login, groups, IF(killed = 'killed', IF(state = 3, 0, 1), 0), id, datetime, ENCODE(password, 'test12345678901234567890') FROM nika_system.abon;

#Bills
INSERT INTO abills.bills(id,uid,deposit,registration) SELECT id, id, depozit, datetime FROM nika_system.abon;

INSERT INTO abills.payments( date, sum, ip, uid, aid, inner_describe ) SELECT NOW(), depozit, INET_ATON('127.0.0.1'),id,2,'Migration' FROM nika_system.abon;

#Internet
INSERT INTO abills.internet_main (uid, ip, cid, port) SELECT user, INET_ATON(ip), mac, port FROM nika_system.comp;

UPDATE abills.internet_main i

  CROSS JOIN
  ( SELECT if(tp.tp_id IS NULL, 111, tp.tp_id) as insert_tp_id, a.id AS nika_uid
    FROM abills.tarif_plans tp
      LEFT JOIN nika_system.abon a ON (a.tarif=tp.id)
  ) AS m
SET i.tp_id=m.insert_tp_id
WHERE i.uid=m.nika_uid;

INSERT INTO abills.internet_main (uid, ip, cid, tp_id, comments) SELECT 2, INET_ATON(ip), mac, 1, CONCAT(name_m, '', type) FROM nika_system.modem WHERE ip LIKE '172.17.%';

INSERT INTO abills.abon_tariffs (id, name, period, price)   SELECT id, name,  IF(dayORmonth='day', 0, 1), IF(dayORmonth='day', cost_day, cost) FROM nika_system.new_tarif WHERE id IN (183, 182, 165, 164, 163, 157, 156, 155, 154);

#Address
INSERT INTO abills.users_pi (uid, fio, fio2, fio3, email, comments, pasport_grant, pasport_num, address_flat, contract_id) SELECT id, first_name, second_name, third_name, email, comment, pass_data, pass, apartment, id FROM nika_system.abon;
UPDATE abills.users_pi SET address_street = (SELECT nika_system.streets.street
                                              FROM nika_system.streets
                                              LEFT JOIN nika_system.abon ON (nika_system.abon.street=nika_system.streets.id)
                                              WHERE (nika_system.abon.id=abills.users_pi.uid));

UPDATE abills.users_pi SET address_flat = (SELECT apartment FROM nika_system.abon WHERE (nika_system.abon.id=abills.users_pi.uid));
UPDATE abills.users_pi SET address_build = (SELECT house FROM nika_system.abon WHERE (nika_system.abon.id=abills.users_pi.uid));

INSERT  INTO abills.districts (`name`, `comments`) VALUES ('Main District', '');
REPLACE INTO abills.streets (name, district_id) SELECT address_street, 1 FROM abills.users_pi group by 1;
REPLACE INTO abills.builds (street_id, number) select s.id, address_build from abills.users_pi u, abills.streets s   WHERE u.address_street=s.name GROUP BY address_street, address_build;

UPDATE abills.users_pi SET _actual_mob = (SELECT actual
                   FROM nika_system.abon
                   WHERE (nika_system.abon.id = abills.users_pi.uid));

UPDATE abills.users_pi SET _actual_mob = (SELECT actual FROM nika_system.abon WHERE (nika_system.abon.id = abills.users_pi.uid));

INSERT INTO abills.users_contacts (uid, type_id, value) SELECT id, 2, phone FROM nika_system.abon  WHERE phone<>'';
INSERT INTO abills.users_contacts (uid, type_id, value) SELECT id, 1, mobile FROM nika_system.abon WHERE mobile<>'';

UPDATE abills.users_pi pi
  LEFT JOIN abills.streets s ON (s.name=pi.address_street)
  LEFT JOIN abills.builds b ON (s.id=b.street_id AND b.number=pi.address_build)
SET pi.location_id=b.id   WHERE pi.location_id=0;


UPDATE _connection_type_list SET id=12 WHERE id=11;
UPDATE abills.users_pi SET _connection_type = (SELECT category FROM nika_system.abon WHERE (nika_system.abon.id = abills.users_pi.uid));


INSERT INTO abills.fees (uid, bill_id, dsc, date, sum, last_deposit)  SELECT s.user, s.user, s.tarif, s.date_start, s.summa, s.summa FROM stat_serv_new s;

  };

  print $sql;

  return 1;
}

#*********************************************************
=head2 custom_1() - Custom ernestas system

=cut
#*********************************************************
sub get_custom_1 {

  my %fields = (
    'id'                      => 'id',
    'timestamp'               => '1.REGISTRATION',
    'sutartis'                => '3.CONTRACT_ID',    #номер контракта
    'vardas'                  => '3.FIO',            #имя
    'pavarde'                 => '3.FIO',            #фамилия
    'miestas'                 => '3.CITY',           #гогрод
    'gatve'                   => '3.ADDRESS_STREET', # улица
    'namas'                   => '3.ADDRESS_BUILD',  # дом
    'butas'                   => '3.ADDRESS_FLAT',   # квартира
    'email'                   => '3.EMAIL',
    'kodas'                   => '1.UID',    #-персональный код человека или компании
    'planas'                  => '4.TP_ID',  # план prioritetas-приоритет ???
    'planas_id'               => '4.TP_NUM', # план ид
    'pasirasyta'              => '',
    'pajungimas'              => '',
    'stop_nuo'                => '',
    'stop_iki'                => '',
    'info'                    => '',
    'user'                    => '',
    'telefonas'               => '',
    'telefonas2'              => '',
    'comment'                 => '',
    'printbill'               => '',
    'saskadresas'             => '',
    'saskemail'               => '',
    'saskinfo'                => '',
    'postaddr'                => '',
    'postcode'                => '',
    'kreipinys'               => '4.STATUS', #неисправности
    'saskpastaba'             => '',
    'susije_asmenys'          => '',
    'sustabdymo_atidejimas'   => '', # отложить отключение
    'auto_stabdymas'          => '', # авто отключенте
    'atidejimo_periodiskumas' => '',
    'mokejimo_atidejimas'     => '',
    'atidej_period_ived_data' => '',
    'filialas'                => '3.BRANCH_OFFICE', # филиал
    's_login'                 => 'LOGIN',
    's_password'              => 'PASSWORD',
    'susije_moketojai'        => '',
    'asmeninis_kreditas'      => '1.CREDIT', #личный предоставленый кредит
    'infopage'                => '',
    'infopage_date'           => '',
    'coord_wgs'               => '',
    'coord_lks'               => '',
    'coord_url'               => '',
    'pozymis1'                => '',
    'pozymis2'                => '',
    'pozymis3'                => '',
    'pozymis4'                => '',
    'pozymis5'                => '',
    'pozymis6'                => '',
    'pozymis7'                => '',
    'pozymis8'                => '',
    'perspejimo_atidejimas'   => '',
    'auto_perspejimas'        => '',
    'kryptis'                 => '',
    'info1'                   => '',
    'info2'                   => '',
    'info3'                   => '',
    'im_kodas'                => '',
    'pvm_kodas'               => '3._TAX_CODE', # ндс код
  );

  my %reverse = reverse(%fields);

  my $fields_list = join(", \n", values(%reverse));

  my $sql = "SELECT $fields_list FROM `klientai`;";
  print "$sql\n" if ($DEBUG > 0);
  my DBI $q = $db->prepare($sql);
  $q->execute();

  #my $query_fields = $q->{NAME};
  #my $output       = '';
  my %logins_hash = ();

  while (my $row = $q->fetchrow_hashref()) {
    my $LOGIN = $row->{id} || q{};
    $logins_hash{$LOGIN}{LOGIN} = $LOGIN;
    #for (my $i = 1; $i < $#row; $i++) {
    foreach my $key (keys %$row) {
      if ($DEBUG > 3) {
        print "$key: $fields{$key} -> ". ($row->{$key} || q{}) ."\n";
      }

      #if(! $fields{ $key }) {
      #print "NO field for key '$key'\n";
      #exit;
      #}

      $logins_hash{$LOGIN}{ $fields{ $key } || '-' } = $row->{$key} || q{};
    }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      $logins_hash{$LOGIN}{$k} = $v;
    }
  }

  undef($q);
  return \%logins_hash;
}

#*********************************************************
=head2 _date_convert($input) - Date convert

=cut
#*********************************************************
sub _date_convert {
  my ($input) = @_;
  my $date = $input;

  if(!$date) {
    return q{};
  }

  if($date =~ /^(\d{4}\-\d{2}\-\d{2})/) {
    $date = $1;
  }
  elsif ($date =~ /^(\d{1,2})\.(\d{1,2})\.(\d{4})/) {
    $date = sprintf("%d-%02d-%02d", $3, $2, $1);
  }

  return $date;
}

#**********************************************************
=head2 int2ip($int) Convert integer value to ip

=cut
#**********************************************************
sub int2ip {
  my $int = shift;

  my $w=($int/16777216)%256;
  my $x=($int/65536)%256;
  my $y=($int/256)%256;
  my $z=$int%256;
  return "$w.$x.$y.$z";

  #Old way
  #  my @d = ();
  #  $d[0] = int($int / 256 / 256 / 256);
  #  $d[1] = int(($int - $d[0] * 256 * 256 * 256) / 256 / 256);
  #  $d[2] = int(($int - $d[0] * 256 * 256 * 256 - $d[1] * 256 * 256) / 256);
  #  $d[3] = int($int - $d[0] * 256 * 256 * 256 - $d[1] * 256 * 256 - $d[2] * 256);
  #return "$d[0].$d[1].$d[2].$d[3]";
}

#*******************************************************************
=head2 get_mikrobill() - Get and parse data from Mikrobill


=cut
#*******************************************************************
sub get_mikrobill {

  my $sql = "
    SELECT *
    FROM stat
  ";

  my DBI $q = $db->prepare($sql);
  $q->execute();

  my %total_data;

  while (my $ref = $q->fetchrow_hashref) {

    $ref->{usrip} =~ s/;//g;
    $ref->{ballance}=~ s/ Грн.//g;
    $ref->{ballance}=~ s/\,/\./g;

    my @speed = split(/ /,$ref->{spdlim});
    $speed[0] =~ s/\,/\./g;
    $speed[0] =~ s/-//g;

    my $stopdate = "$ref->{stopdate}";
    $stopdate =~ s/ //g;

    my $expire = '';
    if ($stopdate ne '01.01.0001') {
        $stopdate = Time::Piece->strptime($stopdate, "%d.%m.%Y");
        $expire = $stopdate->strftime("%Y-%m-%d");
    }

    my @otherinfo = split(/\|\|/,$ref->{'otherinfo'});
    my @personalinfo = split(/\|\|/,$ref->{'pinfo'});

    my $login = $ref->{user_name},
    my $address = $otherinfo[2],
    # my  $address2   = $personalinfo[6],

    my $tel = $otherinfo[3];
    my $email = $otherinfo[4];
    my $mac = $otherinfo[6];

    my $comment    = $otherinfo[9];
    my $comment2   = $otherinfo[25];
    my $perscredit = $otherinfo[34];

    my $passid  = $personalinfo[14];
    my $passport_vidan = $personalinfo[20];
    my $passport_code = $personalinfo[21];
    my $passport_series = $personalinfo[22];
    my $passport_number = $personalinfo[23];
    my $comment3 = $personalinfo[26];
    my $company = $personalinfo[28];

    my %fields = (
      'LOGIN'      => $login,
      'PASSWORD'   => $ref->{user_pswd},

      '1.EXPIRE'   => $expire,
      '1.COMPANY_ID' => $company,
      '1.CREDIT' => $perscredit,
      '1.GID_NAME'    => $ref->{group_guid},

      '3.ADDRESS_FULL' => $address,
      '3.COMMENTS' => "$comment\n$comment2\n$comment3",
      '3.CONTRACT_ID' => $ref->{contract},
      '3.EMAIL' => $email,
      '3.FIO'       => $ref->{FIO},
      '3.PHONE' => $tel,
      '3.PASPORT_NUM' => "$passport_code $passport_series $passport_number",
      '3.PASPORT_DATE' => $passport_vidan,

      '4.CID'  => $mac,
      '4.CPE_MAC' => $mac,
      '4.IP' => $ref->{usrip},
      '4.SPEED' => $speed[0],
      '4.TP_NAME' => $ref->{tarif},
      '4.INTERNET_SKIP_FEE' => 1,

      '5.SUM' => $ref->{ballance},
      '5.DESCRIBE' => 'Migration',

    );

    $total_data{$login} = \%fields;

  }

  undef($q);
  return \%total_data;
}

#**********************************************************
=head2 get_lanbilling($attr) -  Export from Lanbilling

  Arguments:

  Results:

=cut
#**********************************************************
sub get_lanbilling {
  #my ($attr) = @_;

  use utf8;
  my %fields = (
    "LOGIN"                    => "IF(u.type=1, v.login, LOWER(LEFT(IFNULL(u.login, ''), 20))) AS login",
    "PASSWORD"                 => "IF(u.type=1, v.pass, IFNULL(u.pass, 'lanbill_password')) AS password",
    "1.ACTIVATE"               => "IFNULL(cn.date, '0000-00-00') AS registration_1",
    #    "1.EXPIRE"        => "",
    #    "1.COMPANY_ID"    => "",
    "1.CREDIT"                 => "cn.credit AS credit",
    #    "1.CREDIT_DATE"   => "",
    "1.GID"                    => "us.group_id AS group_id",
    "1.GID_NAME"               => "us.name AS group_name",
    #    "1.DISABLE"       => "",
    "1.REGISTRATION"           => "IFNULL(cn.date, '0000-00-00') AS registration_2",
    "1.UID"                    => "IF(u.type=1, '', u.uid) AS uid",

    '3._COMPANY'               => 'u.type AS _company',
    '1.COMPANY_NAME'           => "IF(u.type=1, u.name, '') AS company_name",

    # Company addr
    "1.COMPANY_ADDRESS_BUILD"  => "IFNULL(adr_b.name,'') AS company_building",
    "1.COMPANY_ADDRESS_FLAT"   => "IFNULL(adr_f.name,'') AS company_flat",
    "1.COMPANY_ADDRESS_STREET" => "IFNULL(adr_s.name,'') AS company_street",
    "1.COMPANY_ADDRESS_FULL"   => "IFNULL(adr.address,'') AS company_address",
    "1.COMPANY_ZIP"            => "IFNULL(adr_b.idx,'') AS company_idx",
    "1.COMPANY_CITY"           => "IFNULL(adr_c.name,'') AS company_city",
    "1.COMPANY_COMMENTS"       => "ac.descr AS company_comments",

    #Users / Company Subcompany login addr
    "3.ADDRESS_BUILD" => "IF(u.type=1, vg_adr_b.name, IFNULL(adr_b.name,'')) AS building",
    "3.ADDRESS_FLAT"  => "IF(u.type=1, vg_adr_f.name, IFNULL(adr_f.name,'')) AS flat",
    "3.ADDRESS_STREET"=> "IF(u.type=1, vg_adr_s.name, IFNULL(adr_s.name,'')) AS street",
    "3.ADDRESS_FULL"  => "IF(u.type=1, vg_addr.address, IFNULL(adr.address,'')) AS address",
    "3.ZIP"           => "IF(u.type=1, vg_adr_b.idx, IFNULL(adr_b.idx,'')) AS idx",
    "3.CITY"          => "IF(u.type=1, vg_adr_c.name, IFNULL(adr_c.name,'')) AS city",

    "3.COMMENTS"      => "IF(u.type=1, v.descr, u.descr) AS comments",
    "3.CONTRACT_ID"   => "IFNULL(cn.number, '0') AS contract_id",
    "3.CONTRACT_DATE" => "IFNULL(cn.date, '0000-00-00') AS registration_3",
    "3._CODE"         => "IFNULL(cn.code, '') AS _code",
    "3.EMAIL"         => "u.email AS email",
    "3.FIO"           => "CONVERT(CAST(LEFT(u.name, 120) AS BINARY) USING utf8) AS fio",
    "3.PHONE"         => "IFNULL(u.phone, '') AS phone",
    "3._MOBILE_PHONE" => "u.mobile AS _mobile_phone",
    "3._GIDS"         => "GROUP_CONCAT(DISTINCT(IFNULL(uss.group_id, 0))) AS _gids",
    "3.PASPORT_NUM"   => "IFNULL(CONCAT(ac.pass_sernum,'-',ac.pass_no),'') AS pass_ser_no",
    "3.PASPORT_DATE"  => "IFNULL(CONCAT(ac.pass_issuedate,' ',ac.pass_issueplace),'') AS pass_issuedate",
    "3.PASPORT_GRANT" => "IFNULL(CONCAT(ac.pass_issuedep,' ',ac.pass_issueplace),'') AS pass_issuedepart",
    "3.BIRTH_DATE"    => 'u.birthdate AS birthdate',
    "3.TAX_NUMBER"    => "IFNULL(ac.inn, '') AS inn",

    #"4.CID"           => "IFNULL(de1.value, '') AS cid",
    "4.IP"            => "INET_NTOA(CONV(SUBSTR(HEX(s.segment),25,8),16,10)) AS ip",
    "4.NETMASK"       => "INET_NTOA(CONV(SUBSTR(HEX(s.mask),25,8),16,10)) as nas_mask",
    #    "4.TP_ID"         => "",
    #    "4.TP_ID"         => "",
    "4.TP_NAME"       => "LEFT(t.descr, 60) AS tp_name",
    #"4.STATUS"        => "tp_status",
    "4.PORT"          =>  "po.name AS port",  # "po.port_id AS port",
    "4.COMMENTS"      =>  "po.comment AS internet_comments",  # "po.port_id AS port",
    #

    #"4.NAS_IP"        => "INET_NTOA(CONV(SUBSTR(HEX(s.segment),25,8),16,10)) AS nas_ip",
    #"4.NAS_MAC"      => "INET_NTOA(CONV(SUBSTR(HEX(s.segment),25,8),16,10)) AS nas_ip",
    #    "4.NAS_NAME"      => "",
    '4.NAS_NAME'         => "de1.device_id AS nas_name",
    '4.NAS_MAC'          => "MAX(IF (de1.name = 'Agent-Remote-Id', de1.value, '')) AS nas_mac",
    '4.NAS_IP'           => "MAX(IF (de1.name = 'IP', de1.value, '')) AS nas_ip",
    '4.NAS_MNG_PASSWORD' => "MAX(IF (de1.name = 'Community', de1.value, '')) AS nas_mng_password",

    #"4.NAS_PORT"         => "CONCAT(INET_NTOA(CONV(SUBSTR(HEX(s.segment),25,8),16,10)), '#', po.name) AS nas_port",
    "4.SERVER_VLAN"      => "IFNULL(vl.outer_vlan, 1) AS server_vlan",
    "4.VLAN"             => "IFNULL(vl.inner_vlan, 1) AS vlan",
    #    "4.SIMULTANEONSLY"=> "",
    #    "4.SPEED"         => "",
    #    "4.STATUS"        => "",
    #    "4.PASSWORD"      => "",
    #    "4.LOGIN"         => "",
    #    "4.STATUS"        => "",

    "4.LOGIN"         => 'v.login AS internet_login',
    "4.PASSWORD"      => 'v.pass AS internet_password',

    "5.SUM"           => "cn.balance AS balance",
  # "5.EXT_ID"        => "EXT_ID",
  );

  my %fields_rev = reverse(%fields);

  my $fields_list = "IF(u.type=1, v.login, LOWER(LEFT(IFNULL(u.login, ''), 20))) AS login, " . join(", \n", values(%fields));

  foreach my $key (keys %fields_rev) {
    $key =~ /([a-z\_0-9]+)$/i;
    $fields_rev{$1}=$fields_rev{$key};
  }

  my $WHERE = q{};
  my $LIMIT = q{};
  if ($argv->{LOGIN}) {
    $WHERE = qq{ WHERE u.login LIKE '$argv->{LOGIN}'  };
  }

  if($argv->{LIMIT}) {
    $LIMIT = qq{ LIMIT $argv->{LIMIT} };
  }

  if ($argv->{TP_ID}) {
    $WHERE .= (! $WHERE) ? " WHERE v.tar_id IN ($argv->{TP_ID}) " : " v.tar_id IN ($argv->{TP_ID}) ";
  }

  my $sql = qq{
      SELECT
      $fields_list
    FROM
      accounts u
    LEFT JOIN usergroups_staff uss ON (u.uid = uss.uid)
    LEFT JOIN usergroups us ON (uss.group_id = us.group_id)
    LEFT JOIN agreements cn ON (u.uid = cn.uid)
    LEFT JOIN accounts ac ON (u.uid = ac.uid)

    LEFT JOIN accounts_addr adr ON (u.uid = adr.uid)
    LEFT JOIN address_city adr_c ON (adr.city = adr_c.record_id)
    LEFT JOIN address_street adr_s ON (adr.street = adr_s.record_id)
    LEFT JOIN address_building adr_b ON (adr.building = adr_b.record_id)
    LEFT JOIN address_flat adr_f ON (adr.flat = adr_f.record_id)

    INNER JOIN vgroups v ON (u.uid = v.uid AND v.archive=0)

    LEFT JOIN vgroups_addr vg_addr ON (vg_addr.vg_id = v.vg_id)
    LEFT JOIN address_city vg_adr_c ON (vg_addr.city = vg_adr_c.record_id)
    LEFT JOIN address_street vg_adr_s ON (vg_addr.street = vg_adr_s.record_id)
    LEFT JOIN address_building vg_adr_b ON (vg_addr.building = vg_adr_b.record_id)
    LEFT JOIN address_flat vg_adr_f ON (vg_addr.flat = vg_adr_f.record_id)


    LEFT JOIN tarifs t ON (t.tar_id = v.tar_id)
    LEFT JOIN ports po ON (v.vg_id = po.vg_id)
    LEFT JOIN devices de ON (de.device_id = po.device_id)
    LEFT JOIN staff s ON (v.vg_id = s.vg_id AND s.archive = 0)
    LEFT JOIN vlans vl ON (po.vlan_id = vl.record_id)
    LEFT JOIN devices_options de1 ON (de.device_id = de1.device_id)
      $WHERE
      GROUP BY u.uid, v.vg_id
      ORDER BY u.uid
      $LIMIT
  };

  # if ($argv->{LIMIT}) {
  #   $sql .= " LIMIT $argv->{LIMIT}";
  # }

  if ($DEBUG > 4) {
    print $sql;
    return 0;
  }
  elsif ($DEBUG > 0) {
    print "$sql\n";
  }

  $db->do("SET sql_mode='';");

  my DBI $q = $db->prepare($sql);
  $q->execute();
  my $query_fields = $q->{NAME};

  my %logins_hash = ();

  while (my @row = $q->fetchrow_array()) {
    my $LOGIN = $row[0];

    for (my $i = 0; $i < $#row; $i++) {
      my $field_id = $fields_rev{$query_fields->[$i]};
      my $val = $row[$i] || q{};
      if ($DEBUG > 3) {
        print "NUM: $i QUERY_FIELD_NAME: $query_fields->[$i], $field_id -> $row[$i]\n";
      }

      Encode::_utf8_on($val);

      # Extract main group
      $argv->{GID_PRIORITY} = "2,7,5,6";
      if ($field_id eq '3._GIDS' && $argv->{GID_PRIORITY}) {
        foreach my $group_id ( split(/,\s?/, $argv->{GID_PRIORITY}) ){

          if($val =~ /$group_id/) {
            $logins_hash{$LOGIN}{'1.GID'} = $group_id;
            $logins_hash{$LOGIN}{'1.GID_NAME'} = $group_id;
            last;
          }
        }
      }

      if ($logins_hash{$LOGIN} && $logins_hash{$LOGIN}{$field_id}) {
        if ($logins_hash{$LOGIN.'__2'} && $logins_hash{$LOGIN.'__2'}{$field_id}) {
          $logins_hash{$LOGIN . '__3'}{$field_id} = $val;
        }
        else {
          $logins_hash{$LOGIN . '__2'}{$field_id} = $val;
          `echo "extra login $LOGIN" >> extra_login.txt`;
        }
      }
      else {
        $logins_hash{$LOGIN}{$field_id} = $val;
      }
    }

    if ($logins_hash{$LOGIN}{'3.COMMENTS'}) {
      $logins_hash{$LOGIN}{'3.COMMENTS'} =~ s/\n//g;
    }

    #Extended params
    while (my ($k, $v) = each %EXTENDED_STATIC_FIELDS) {
      if ($logins_hash{$LOGIN} && $logins_hash{$LOGIN}{$k}) {
        $logins_hash{$LOGIN .'__2'}{$k} = $v;
      }
      else {
        $logins_hash{$LOGIN}{$k} = $v;
      }
    }
  }
  #use Data::Dumper;
  #print Dumper(\%logins_hash);
  #  $q = $db->prepare("SHOW variables like '%char%';");
  #$q->execute();
  #while (my @row = $q->fetchrow_array()) {
  #  print "$row[0] -> $row[1]\n";
  #}

  undef($q);
  return \%logins_hash;
}

1
