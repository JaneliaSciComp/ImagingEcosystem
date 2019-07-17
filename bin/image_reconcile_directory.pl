#!/usr/bin/perl

# A quick&dirty program to crawl a directory tree, look for LSMs, and move
# them to another directory if that have a JFS path and the file exists on
# Scality.
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
use JSON;
use LWP::Simple;

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/opt/informatics/data/';

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
    print "No response for image $name at $_\n";
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
      print "$name is on Scality and /dm11\n";
      $count{'Files on Scality and /dm11'}++;
    }
    else {
      &checkScality($File::Find::name,$name,$url);
    }
  }
  elsif ($path) {
    print "$name is on /dm11 only\n" if ($DEBUG);
    $count{'Files on /dm11'}++;
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
  my($full_path,$name,$url) = @_;
  my $on_scality = head($url);
  if ($on_scality) {
    print "$name was copied to Scality but not removed from /dm11\n";
    $count{'Files needing deletion from /dm11'}++;
    if ($WRITE) {
      my($dir,$file) = split('/',$name);
      unless (-e($NEW_PATH."/$dir")) {
        my @made = make_path($NEW_PATH."/$dir");
      }
      move($full_path,$NEW_PATH."/$dir");
    }
  }
  else {
    print "$name has a JFS path but does not exist on Scality\n";
    $count{'Files on Scality without working URL'}++;
  }
}


sub getREST
{
  my($rest) = shift;
  my $response = get $rest;
  #&terminateProgram("<h3>REST GET returned null response</h3>"
  #                  . "<br>Request: $rest<br>")
  #  unless (length($response));
  unless ($response) {
    print "No response for call $rest\n";
    return();
  }
  return() unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                    . "Response: $response<br>Error: $@") if ($@);
  return($rvar);
}
