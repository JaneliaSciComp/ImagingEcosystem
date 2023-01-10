#!/usr/bin/perl

# A quick&dirty program to crawl a directory tree, look for LSMs, and move
# them to another directory if that have a JFS path and the file exists on
# archive.
# If a file is not found in SAGE, this program will see if it's elegible for
# deletion (due to a rename), and will delete it if applicable.

use strict;
use warnings;
use Cwd;
use DBI;
use JFRC::Utils::DB qw(:all);
use File::Copy;
use File::Find;
use File::Path qw(make_path);
use Getopt::Long;
use HTTP::Request;
use JSON;
use LWP::Simple;
use LWP::UserAgent;

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/groups/scicompsoft/informatics/data/';

my ($DEBUG,$VERBOSE,$WRITE) = (0)x3;
my ($NEW_PATH,$PATH);

our $dbh;
my %sth = (
  ERRORS => "SELECT msg FROM logs WHERE program='daemon' AND datetime >= ? AND priority IN ('err','warning') AND msg LIKE ? ORDER BY seq",
  SUCCESS => "SELECT msg FROM logs WHERE program='daemon' AND datetime >= ? AND priority='info' AND msg LIKE ? ORDER BY seq",
);
my (%count,%REST);

GetOptions('path=s'     => \$PATH,
           'new_path=s' => \$NEW_PATH,
           write        => \$WRITE,
           verbose      => \$VERBOSE,
           debug        => \$DEBUG);
$NEW_PATH ||= '/groups/flylight/flylight/lsm_archive';
$PATH ||= getcwd;

&initialize();
find(\&wanted,$PATH);
foreach (sort keys %count) {
  print "$_: $count{$_}\n";
}
&terminateProgram();

sub terminateProgram
{
  my($msg) = shift || 0;
  print "$msg\n" if ($msg);
  exit ($msg) ? -1 : 0;
}


sub initialize
{
  # Get general REST config
  my $file = DATA_PATH . 'rest_services.json';
  open SLURP,$file or &terminateProgram("Can't open $file: $!");
  sysread SLURP,my $slurp,-s SLURP;
  close(SLURP);
  my $hr = decode_json $slurp;
  %REST = %$hr;
  # Open syslog
  &dbConnect(\$dbh,'syslog');
  ($sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)) foreach (keys %sth);
}


sub wanted
{
  return unless (/\.lsm/);
  (my $date = $File::Find::dir) =~ s/.+\///;
  (my $name = $_ ) =~ s/\.bz2$//;
  $name = "$date/$name";
  $count{'Files found'}++;
  print "$name\n" if ($DEBUG);
  my $rvar = &getREST($REST{sage}{url}."images?name=$name");
  unless ($rvar) {
    $count{"Files not in SAGE"} += 1;
    return;
  }
  my ($jfs_path,$path,$url) = ('')x3;
  unless (scalar @{$rvar->{image_data}}) {
    &checkForRename($File::Find::name,$name);
    return;
  }
  foreach (@{$rvar->{image_data}}) {
    $path = $_->{path} || '';
    $jfs_path = $_->{jfs_path} || '';
    $url = $_->{image_url} || '';
  }
  if ($jfs_path) {
    if ($path) {
      print "$name is on archive and /dm11\n";
      $count{'Files on archive and /dm11'}++;
    }
    else {
      &checkScality($File::Find::name,$name,$jfs_path,$url);
    }
  }
  elsif ($path) {
    print "$name is on /dm11 only\n" if ($DEBUG);
    $count{'Files to remain on /dm11'}++;
  }
  else {
    print "$name has no paths\n";
    $count{'Files with no path'}++;
  }
}


sub checkForRename
{
  my($path,$name) = @_;
  my($date,$lsm) = split('/',$name);
  $date = sprintf '%s-%s-%s',substr($date,0,4),substr($date,4,2),substr($date,6,2);
  $sth{ERRORS}->execute($date,"%$lsm%");
  my $ar = $sth{ERRORS}->fetchall_arrayref();
  print "$name is not in SAGE\n";
  unless (scalar @$ar) {
    $count{'Files not in SAGE (no errors in syslog)'}++;
    return;
  }
  if (($ar->[0][0] =~ /Failed to copy/) && ($ar->[-1][0] =~ /Failed to remove/)) {
    if ($ar->[0][0] =~ /Failed to copy (.+) to /) {
      my($original) = (split(/\\/,$1))[-1];
      $sth{SUCCESS}->execute($date,"%Successfully copied%$original%");
      my $ar2 = $sth{SUCCESS}->fetchall_arrayref();
      if (scalar @$ar2) {
        print "  $path was re-TMOGged and can be deleted\n";
        $count{'Files renamed'}++;
        return;
      }
    }
  }
  $count{'Files not in SAGE'}++;
}


sub checkScality
{
  my($full_path,$name,$jfs_path,$url) = @_;
  my $on_scality;
  if (index($url,'img.int.janelia.org') != -1) {
    #print "$name has a JFS path but old URL $url\n";
    $count{'Files with old URL'}++;
    return;
  }
  # We attempt access with the URL because tmog can't access the archive location.
  #print "$name\n$full_path\n$url\n";
  $url =~ s/.+\/api\/file/https:\/\/workstation.int.janelia.org\/SCSW\/JADEServices\/v1\/storage_content\/storage_path_redirect/;
  #eval { $on_scality = head($url); };
  #print "Eval: $@\n";
  #my ($type, $length, $mod) = head($url);
  #print "$type, $length, $mod\n";
  #if ($on_scality) {
  my $ua = LWP::UserAgent->new(ssl_opts => {
    verify_hostname => 0,
  });
  my $req = $ua->head($url);
  if ($req->is_success) {
    print "$name was copied to archive but not removed from /dm11\n" if ($VERBOSE);
    $count{'Files needing deletion from /dm11'}++;
    my($dir,$file) = split('/',$name);
    if ($WRITE) {
      unless (-e($NEW_PATH."/$dir")) {
        my @made = make_path($NEW_PATH."/$dir");
      }
      move($full_path,$NEW_PATH."/$dir");
    }
    print "Move $full_path to $NEW_PATH/$dir\n" if ($VERBOSE);
  }
  else {
    print "Non-reachable URL $url\n";
    print $req->status_line . "\n";
    $count{'Files on archive without working URL'}++;
  }
}


sub getREST
{
  my($rest) = shift;
  my $request = HTTP::Request->new(GET => $rest);
  my $ua = LWP::UserAgent->new;
  my $response = $ua->request($request);
  if ($response->code == '404') {
    return;
  }
  elsif ($response->code != '200') {
    print "$response->code $rest\n";
    return();
  }
  return() unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response->content)};
  &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                    . "Response: $response<br>Error: $@") if ($@);
  return($rvar);
}
