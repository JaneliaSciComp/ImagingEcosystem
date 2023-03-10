#!/bin/env perl
use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use DBI;
use JSON;
use XML::Simple;

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
use constant DATA_PATH  => '/groups/scicompsoft/informatics/data/';
my $FLYF_SERVER = 'dbi:JDBC:hostname=<REPLACE>;port=9001';
my $FLYF_URL = 'jdbc:filemaker://<REPLACE>/FLYF_2?user=flyf2&password=flycore';

# ****************************************************************************
# * Global variables                                                         *
# ****************************************************************************
our $dbhf;
my %sthf = (
  AGING => "SELECT Stock_Name,date_create,date_modified FROM StockFinder "
           . "WHERE Stock_Name IS NOT NULL AND date_modified IS NOT NULL "
           . "AND (Hour(Timestamp()-date_modified)*60+Minute(Timestamp()-"
           . "date_modified))<=? AND (Hour(Timestamp()-date_modified)*60+"
           . "Minute(Timestamp()-date_modified))>? ORDER BY 3 DESC",
  DOILIST => "SELECT DISTINCT doi FROM StockFinder WHERE doi IS NOT NULL",
  INITIAL_SPLITS => "SELECT Barcode_CrossSerialNumber AS cross_barcode,"
                    . "initial_split AS line,stockname_reserved AS genotype,Project AS project "
                    . "FROM Project_Crosses WHERE project IN ('Fly Light','FLE') AND "
                    . "cross_type IN ('SplitGAL4','IntermediateSplit') AND "
                    . "initial_split LIKE 'JRC_I%'",
  INITIAL_SPLIT => "SELECT Barcode_CrossSerialNumber AS cross_barcode,"
                   . "initial_split AS line,stockname_reserved AS genotype,Project AS project "
                   . "FROM Project_Crosses WHERE project IN ('Fly Light','FLE') AND "
                   . "cross_type IN ('SplitGAL4','IntermediateSplit') AND "
                   . "initial_split=?",
  LINELIST => "SELECT Stock_Name FROM StockFinder WHERE Stock_Name IS NOT NULL",
  LAB => 'SELECT "__kp_UniqueID",Production_Info,Quality_Control,"Lab ID" FROM StockFinder WHERE Stock_Name=?',
  CROSSDATA => 'SELECT * FROM Project_Crosses WHERE "__kp_ProjectCrosses_Serial_Number"=?',
  CROSSDATATMOG => 'SELECT "__kp_ProjectCrosses_Serial_Number" AS serialNumber,'
                   . 'c.stockname_reserved AS stocknameReserved,'
                   . 'c.Barcode_CrossSerialNumber AS barcodeCrossSerialNumber,'
                   . 'c.Lab_Project AS labProject,c.lab_member AS labMember,'
                   . 'c.Crossed_Notes AS crossDescription,'
                   . 's1.Stock_Name AS parent1StockName,'
                   . 's2.Stock_Name AS parent2StockName,'
                   . 'c.special_cross_label AS specialCrossLabel,'
                   . 'c.Reporter AS reporter,c.initial_split '
                   . 'FROM Project_Crosses c '
                   . 'LEFT JOIN StockFinder s1 ON s1."__kp_UniqueID"=c."_kf_Parent_UID" '
                   . 'LEFT JOIN StockFinder s2 ON s2."__kp_UniqueID"=c."_kf_Parent2_UID" '
                   . 'WHERE c."__kp_ProjectCrosses_Serial_Number"=?',
  LINEDATA => "SELECT * FROM StockFinder WHERE Stock_Name=?",
  LOCATION => 'SELECT * FROM "__flipper_flystocks_stock" WHERE STOCK_ID=?',
  NAMED_STOCKS => 'SELECT "__kp_UniqueID",Stock_Name FROM StockFinder WHERE Stock_Name IS NOT NULL AND Stock_Name!='
                  . "'KEEP EMPTY'",
  NAMES => 'SELECT all_names,published,label,display_genotype,who,create_date,notes FROM all_names WHERE "_kf_Parent_UID"=?',
  PUBLISHING => 'SELECT "_kf_parent_UID","__kp_name_serial_number",'
                . 'all_names,for_publishing,published,label,display_genotype,who,notes,'
                . "create_date FROM all_names WHERE for_publishing='Yes'",
  PUBLISHING_ALL => 'SELECT "_kf_parent_UID","__kp_name_serial_number",'
                    . 'all_names,for_publishing,published,label,display_genotype,who,notes,'
                    . "create_date FROM all_names",
  PUBLISHING_48=> 'SELECT "_kf_parent_UID","__kp_name_serial_number",'
                    . 'all_names,for_publishing,published,label,display_genotype,who,notes,'
                    . "create_date FROM all_names WHERE Hour(Timestamp()-create_date)/24 <= 30",
  PUBLISH_JOIN => 'SELECT a."_kf_parent_UID",a."__kp_name_serial_number",a.all_names,a.for_publishing,'
                  . 'a.published,a.label,a.display_genotype,a.who,a.notes,a.create_date,s.Stock_Name FROM all_names a '
                  . 'JOIN StockFinder s ON s."__kp_UniqueID"=a."_kf_Parent_UID" WHERE s.Stock_Name IS NOT NULL '
                  . "AND a.for_publishing='Yes' AND Hour(Timestamp()-a.create_date)/24 <= 220",
  STABLE_SPLITS => "SELECT DISTINCT sf.Stock_Name,pc.Cross_Type FROM Project_Crosses pc,"
                   . "StockFinder sf WHERE sf.Stock_Name LIKE 'JRC\_S%' AND "
                   . "pc.Cross_Type IN ('SplitFlipOuts','SplitPolarity',"
                   . "'StableSplitScreen') AND "
                   . 'pc."_kf_Parent_UID"=sf."__kp_UniqueID"',
);


sub errorResponse
{
  my %response = (error => shift);
  print encode_json(\%response);
  exit(-1);
}


my $Line = param('line') || '';
my $Robot_id = param('robot_id') || '';
my $Kp = param('kp') || '';
my $Cross_barcode = param('cross_barcode') || '';
if (!$Line) {
  my $sub = '';
  $sub = 'RobotID=?' if ($Robot_id);
  $sub = '"__kp_UniqueID"=?' if ($Kp);
  if ($sub) {
    foreach (keys %sthf) {
      $sthf{$_} =~ s/Stock_Name=\?/$sub/;
    }
  }
}

# Connect to FLYF2
my $file = DATA_PATH . 'servers.json';
open SLURP,$file or &terminateProgram("Can't open $file: $!");
sysread SLURP,my $slurp,-s SLURP;
close(SLURP);
my $hr = decode_json $slurp;
my %REST = %$hr;
$FLYF_URL =~ s/<REPLACE>/$REST{FlyCore}{address}/;
$FLYF_URL =~ s/([=;])/uc sprintf("%%%02x",ord($1))/eg;
$FLYF_SERVER =~ s/<REPLACE>/$REST{JDBC}{address}/;
$dbhf = DBI->connect(join(';url=',$FLYF_SERVER,$FLYF_URL),(undef)x2)
  or &errorResponse($DBI::errstr);
$sthf{$_} = $dbhf->prepare($sthf{$_})
  || &errorResponse($dbhf->errstr . " Query=$sthf{$_}") foreach (keys %sthf);

# Retrieve data from FLYF2
my %response = (error => '');
my $REQUEST = param('request');
my $FORMAT = lc(param('format')) || 'json';
if ($FORMAT eq 'xml') { # || $REQUEST eq 'crossdatatmog') {
  print header('application/xml');
}
else {
  print header('application/json');
}
my $ROOT = '';
if ($REQUEST eq 'doilist') {
  # DOIs
  $response{$ROOT = 'dois'} = [];
  $sthf{uc($REQUEST)}->execute();
  my $ar = $sthf{uc($REQUEST)}->fetchall_arrayref();
  foreach (@$ar) {
    push @{$response{$ROOT}},$_->[0];
  }
}
elsif ($REQUEST eq 'initial_splits') {
  # Stock names (StockFinder records)
  $response{$ROOT = 'splits'} = [];
  $sthf{uc($REQUEST)}->execute();
  my $hr = $sthf{uc($REQUEST)}->fetchall_hashref('cross_barcode');
  $response{$ROOT} = [map { $hr->{$_} } sort keys %$hr] if ($hr);
}
elsif ($REQUEST eq 'initial_split') {
  # Stock names (StockFinder records)
  $response{$ROOT = 'splits'} = [];
  $sthf{uc($REQUEST)}->execute($Line);
  my $hr = $sthf{uc($REQUEST)}->fetchall_hashref('cross_barcode');
  $response{$ROOT} = [map { $hr->{$_} } sort keys %$hr] if ($hr);
}
elsif ($REQUEST eq 'lab') {
  # Lab
  &errorResponse('Required parameter line, kp, or robot_id is missing') unless ($Line || $Robot_id || $Kp);
  $sthf{uc($REQUEST)}->execute($Line || $Kp || $Robot_id);
  my($flycore,$prod,$qc,$labid) = $sthf{uc($REQUEST)}->fetchrow_array();
  if (defined $flycore) {
    $prod ||= '';
    $labid ||= '';
    $response{'__kp_UniqueID'} = $flycore;
    $response{Production_Info} = $prod;
    $response{Quality_Control} = ($qc || '');
    $response{lab} = $labid;
  }
  else {
    $response{'__kp_UniqueID'} = $response{lab} = '';
  }
}
elsif ($REQUEST eq 'linedata') {
  # Line data (StockFinder record)
  &errorResponse('Required parameter line, kp, or robot_id is missing') unless ($Line || $Robot_id || $Kp);
  $response{$ROOT = 'linedata'} = '';
  $sthf{uc($REQUEST)}->execute($Line || $Kp || $Robot_id);
  my $hr = $sthf{uc($REQUEST)}->fetchrow_hashref();
  if ($hr) {
    $hr->{$_} ||= '' foreach (keys %$hr);
    $hr->{Genotype_GSI_Name_PlateWell} =~ s/\r/ /g;
    $hr->{Genotype_GSI_Name_PlateWell} =~ s/\n/ /g;
    $hr->{Hide} = ($hr->{Hide}) ? 'Y' : 'N';
    $response{$ROOT} = $hr;
  }
}
elsif ($REQUEST eq 'crossdata') {
  # Cross data (Project_Crosses record)
  &errorResponse('Required parameter cross_barcode is missing') unless ($Cross_barcode);
  $response{$ROOT = 'crossdata'} = '';
  $sthf{uc($REQUEST)}->execute($Cross_barcode);
  my $hr = $sthf{uc($REQUEST)}->fetchrow_hashref();
  if ($hr) {
    $hr->{$_} ||= '' foreach (keys %$hr);
    $response{$ROOT} = $hr;
  }
}
elsif ($REQUEST eq 'crossdatatmog') {
  # Cross data (Project_Crosses record)
  &errorResponse('Required parameter cross_barcode is missing') unless ($Cross_barcode);
  $response{$ROOT = 'projectCross'} = {};
  $sthf{uc($REQUEST)}->execute($Cross_barcode);
  my $hr = $sthf{uc($REQUEST)}->fetchrow_hashref();
  $response{$ROOT} = '';
  if ($hr) {
    $hr->{$_} ||= '' foreach (keys %$hr);
    $response{$ROOT} = $hr;
  }
}
elsif ($REQUEST eq 'linelist') {
  # Stock names (StockFinder records)
  $response{$ROOT = 'lines'} = [];
  $sthf{uc($REQUEST)}->execute();
  my $ar = $sthf{uc($REQUEST)}->fetchall_arrayref();
  if (scalar @$ar) {
    foreach (@$ar) {
      my $l = $_->[0];
      next if ($l eq 'pBDPGAL4U' || $l eq 'KEEP EMPTY');
      next if ((index($l,'BAD LINE') > -1) || (index($l,'DO NOT USE') > -1)
               || (index($l,'pull later') > -1));
      push @{$response{$ROOT}},$l;
    }
  }
}
elsif ($REQUEST eq 'location') {
  # Location (__flipper_flystocks_stock records)
  &errorResponse('Required parameter robot_id is missing') unless ($Robot_id);
  $response{$ROOT = 'location'} = '';
  $sthf{LOCATION}->execute($Robot_id);
  my $hr = $sthf{LOCATION}->fetchrow_hashref();
  if ($hr) {
    $hr->{$_} ||= '' foreach (keys %$hr);
    $response{$ROOT} = $hr;
  }
}
elsif ($REQUEST eq 'named_stocks') {
  $response{$ROOT = 'stocks'} = '';
  $sthf{NAMED_STOCKS}->execute();
  my $hr = $sthf{NAMED_STOCKS}->fetchall_hashref("__kp_UniqueID");
  $response{$ROOT} = $hr;
}
elsif ($REQUEST eq 'aging') {
  my $delay_min = (param('delay') || 1) * 60;
  my $Hours = param('hours') || 1;
  my $aging_min = ($Hours * 60) + $delay_min + 5;
  $response{$ROOT = 'lines'} = [];
  $sthf{AGING}->execute($aging_min,$delay_min);
  my $ar = $sthf{AGING}->fetchall_arrayref();
  foreach (@$ar) {
      my %i = (line => $_->[0],
               created => $_->[1],
               updated => $_->[2]);
      push @{$response{$ROOT}},\%i;
  }
}
elsif ($REQUEST eq 'names') {
  # Publishing names (all_names records)
  &errorResponse('Required parameter line, kp, or robot_id is missing') unless ($Line || $Robot_id || $Kp);
  $sthf{LINEDATA}->execute($Line || $Kp || $Robot_id);
  my $hr = $sthf{LINEDATA}->fetchrow_hashref();
  if (scalar keys %$hr) {
    $response{$ROOT = 'names'} = [];
    $sthf{uc($REQUEST)}->execute($hr->{'__kp_UniqueID'});
    my $ar = $sthf{uc($REQUEST)}->fetchall_arrayref();
    foreach (@$ar) {
      my %i = (name => $_->[0],
               published => $_->[1],
               label => $_->[2],
               display_genotype => $_->[3],
               requester => $_->[4],
               create_date => $_->[5],
               notes => $_->[6]);
      push @{$response{$ROOT}},\%i;
    }
  }
}
elsif ($REQUEST eq 'publishing_names') {
  $response{$ROOT = 'publishing'} = '';
  $sthf{PUBLISHING}->execute();
  my $ar = $sthf{PUBLISHING}->fetchall_arrayref();
  $response{$ROOT} = $ar;
}
elsif ($REQUEST eq 'publishing_names_all') {
  $response{$ROOT = 'publishing'} = '';
  $sthf{PUBLISHING_ALL}->execute();
  my $ar = $sthf{PUBLISHING_ALL}->fetchall_arrayref();
  $response{$ROOT} = $ar;
}
elsif ($REQUEST eq 'publishing_names_48') {
  $response{$ROOT = 'publishing'} = '';
  $sthf{PUBLISHING_48}->execute();
  my $ar = $sthf{PUBLISHING_48}->fetchall_arrayref();
  $response{$ROOT} = $ar;
}
elsif ($REQUEST eq 'publishing_names_join') {
  $response{$ROOT = 'publishing'} = '';
  $sthf{PUBLISH_JOIN}->execute();
  my $ar = $sthf{PUBLISH_JOIN}->fetchall_arrayref();
  $response{$ROOT} = $ar;
}
elsif ($REQUEST eq 'stable_splits') {
  # Stock names and cross types (StockFinder and Project_Crosses records)
  $response{$ROOT = 'splits'} = [];
  $sthf{uc($REQUEST)}->execute();
  my $hr = $sthf{uc($REQUEST)}->fetchall_hashref('Stock_Name');
  $response{$ROOT} = [map { $hr->{$_} } sort keys %$hr] if ($hr);
}
else {
    $response{error} = "Unknown request type $REQUEST";
}
if ($FORMAT eq 'xml') {
  print XMLout($response{$ROOT}, RootName => $ROOT,KeepRoot => 1,NoAttr => 1);
}
else {
  print encode_json(\%response);
}
exit(0);
