#!/usr/bin/perl

# A quick&dirty program to crawl a directory tree, look for LSMs, and move
# them to another directory if that have a JFS path and the file exists on
# Scality.

use strict;
use warnings;
use Cwd;
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

my %REST;

GetOptions('path=s'     => \$PATH,
           'new_path=s' => \$NEW_PATH,
           write        => \$WRITE,
           verbose      => \$VERBOSE,
           debug        => \$DEBUG);
$NEW_PATH ||= '/groups/flylight/flylight/lsm_archive';
$PATH ||= getcwd;

&initialize();
find(\&wanted,$PATH);
&terminateProgram();

sub terminateProgram
{
  my($msg) = shift;
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
}


sub wanted
{
  return unless (/\.lsm/);
  (my $date = $File::Find::dir) =~ s/.+\///;
  (my $name = $_ ) =~ s/\.bz2$//;
  $name = "$date/$name";
  print "$name\n" if ($DEBUG);
  my $rvar = &getREST($REST{sage}{url}."images?name=$name");
  my ($jfs_path,$path,$url) = ('')x3;
  foreach (@{$rvar->{image_data}}) {
    $path = $_->{path} || '';
    $jfs_path = $_->{jfs_path} || '';
    $url = $_->{image_url} || '';
  }
  if ($jfs_path) {
    if ($path) {
      print "$name is on Scality and /dm11\n";
    }
    else {
      &checkScality($File::Find::name,$name,$url);
    }
  }
  elsif ($path) {
    print "$name is on /dm11 only\n";
  }
}


sub checkScality
{
  my($full_path,$name,$url) = @_;
  my $on_scality = head($url);
  if ($on_scality) {
    print "$name was copied to Scality but not removed from /dm11\n";
    my($dir,$file) = split('/',$name);
    my @made = make_path($NEW_PATH."/$dir");
    move($full_path,$NEW_PATH."/$dir") if ($WRITE);
  }
  else {
    print "$name has a JFS path but does not exist on Scality\n";
  }
}


sub getREST
{
  my($rest) = shift;
  my $response = get $rest;
  &terminateProgram("<h3>REST GET returned null response</h3>"
                    . "<br>Request: $rest<br>")
    unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                    . "Response: $response<br>Error: $@") if ($@);
  return($rvar);
}
