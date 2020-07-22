#!/usr/bin/perl

# Perl built-ins
use strict;
use warnings;
use DBI;
use Getopt::Long;
use IO::File;
use JSON;
use LWP::Simple;
use Pod::Text;
use Pod::Usage;
use POSIX qw(strftime);

# JFRC
use JFRC::Utils::DB qw(:all);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/groups/scicompsoft/informatics/data/';

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
my %CONFIG;

# ****************************************************************************
# * Global variables                                                         *
# ****************************************************************************
# Command-line parameters
my($DEBUG,$MONGO,$TEST,$VERBOSE) = (0)x4;

# Database
our $dbh;
my %sth = (
STATUS => "SELECT value,COUNT(1) FROM entityData WHERE entity_att='Status' GROUP BY 1",
);


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************

# Get the command-line parameters
GetOptions('log=s'    => \my $log_file,
           'output=s' => \my $output_file,
           test       => \$TEST,
           verbose    => \$VERBOSE,
           debug      => \$DEBUG,
           help       => \my $HELP)
  or pod2usage(-1);

$log_file ||= '/groups/scicompsoft/informatics/logs/workstation_status.log';
# Display help and exit if the -help parm is specified
pod2text($0),&terminateProgram() if ($HELP);

# Initialize
$VERBOSE = 1 if ($DEBUG);
# Get WS REST config
my $file = DATA_PATH . 'workstation_ng.json';
open SLURP,$file or &terminateProgram("Can't open $file: $!");
sysread SLURP,my $slurp,-s SLURP;
close(SLURP);
my $hr = decode_json $slurp;
%CONFIG = %$hr;
$MONGO = ('mongo' eq $CONFIG{data_source});
our $handle = ($output_file) ? (new IO::File $output_file,'>'
                or &terminateProgram("ERROR: could not open $output_file ($!)"))
                           : (new_from_fd IO::File \*STDOUT,'>'
                or &terminateProgram("ERROR: could not open STDOUT ($!)"));
open(STDERR,'>&='.fileno($handle))
    or &terminateProgram("ERROR: could not alias STDERR ($!)");
autoflush $handle 1;
our $lhandle = new IO::File $log_file,'>>'
  or &terminateProgram("ERROR: could not open $log_file for write ($!)");
&dbConnect(\$dbh,'workstation');
$sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
  foreach (keys %sth);

my $ar;
if ($MONGO) {
    my $rest = $CONFIG{url}.$CONFIG{query}{SampleStatus};
    my $response = get $rest;
    &terminateProgram("<h3>REST GET returned null response</h3>"
                      . "<br>Request: $rest<br>")
      unless (length($response));
    my $rvar;
    eval {$rvar = decode_json($response)};
    &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                      . "Response: $response<br>Error: $@") if ($@);
    foreach (@$rvar) {
      push @$ar,[@{$_}{qw(_id count)}] if ($_->{_id});
    }

}
else {
  $sth{STATUS}->execute();
  $ar = $sth{STATUS}->fetchall_arrayref();
}

my $today = strftime "%Y-%m-%d",localtime;
my %found;
foreach (@$ar) {
  $found{$_->[0]}++;
  print {($TEST) ? $handle : $lhandle} "$today\t$_->[0]\t$_->[1]\n";
}
foreach (qw(Blocked Desync Error Processing Retired),'Marked for Rerun') {
  next if (exists $found{$_});
  print {($TEST) ? $handle : $lhandle} "$today\t$_\t0\n";
}

# We're done!
&terminateProgram(0);

# ****************************************************************************
# * Subroutine:  terminateProgram                                            *
# * Description: This routine will gracefully terminate the program. If a    *
# *              message is passed in, we exit with a code of -1. Otherwise, *
# *              we exit with a code of 0.                                   *
# *                                                                          *
# * Parameters:  message: the error message to print                         *
# * Returns:     NONE                                                        *
# ****************************************************************************
sub terminateProgram
{
  my $message = shift;
  print { $handle || \*STDERR } "$message\n" if ($message);
  $lhandle->close if ($lhandle);
  $handle->close if ($handle);
  exit(($message) ? -1 : 0);
}
