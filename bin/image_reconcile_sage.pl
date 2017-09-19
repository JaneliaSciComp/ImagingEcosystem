#!/usr/bin/perl

# A quick&dirty program to find LSM images in SAGE, and verify that the
# corresponding file exists on /dm11 or Scality.

use strict;
use warnings;
use DBI;
use Getopt::Long;
use JSON;
use LWP::Simple;

use JFRC::Utils::DB qw(:all);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/opt/informatics/data/';

my ($VERBOSE,$WRITE) = (0)x2;

my %REST;
our $dbh;
my %sth = (
  LSMS => "SELECT id,family,data_set,name,image_url,path,jfs_path from image_data_mv WHERE data_set IS NOT NULL AND family NOT IN ('dickson_vienna','rubin_ssplit','simpson_descending') ORDER BY 2,3",
);

GetOptions(write   => \$WRITE,
           verbose => \$VERBOSE);

&initialize();
&processImages();
&terminateProgram();

sub terminateProgram
{
  my($msg) = @_;
  print "$msg\n" if ($msg);
  exit(($msg) ? -1 : 0);
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
  &dbConnect(\$dbh,'sage');
  $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
    foreach (keys %sth);
}


sub processImages
{
  $sth{LSMS}->execute();
  my $ar = $sth{LSMS}->fetchall_arrayref();
  foreach (@$ar) {
    my($id,$family,$dataset,$name,$url,$path,$jfs_path) = @$_;
    print "$family\t$dataset\t$name\n" if ($VERBOSE);
    if ($path) {
      if ($jfs_path) {
        print "UPDATE IMAGE SET PATH=NULL WHERE id=$id;\n";
        print "$path\t$jfs_path\t$url\tURL not accessible\n" unless (head($url));
      }
      else {
        $jfs_path ||= 'NULL';
        print "$path\t$jfs_path\t$url\tPath not accessible\n" unless (-e $path);
      }
    }
    elsif ($jfs_path) {
      $path ||= 'NULL';
      if ($url =~ /img\.int/) {
        print "UPDATE image SET url=CONCAT('http://jacs-webdav:8080/JFS/api/file',$jfs_path) WHERE id=$id\n";
      }
      else {
        print "$path\t$jfs_path\t$url\tURL not accessible\n" unless (head($url));
      }
    }
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
