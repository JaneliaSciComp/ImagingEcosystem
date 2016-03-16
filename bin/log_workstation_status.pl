#!/usr/bin/perl

# Perl built-ins
use strict;
use warnings;
use DBI;
use Getopt::Long;
use IO::File;
use Pod::Text;
use Pod::Usage;
use POSIX qw(strftime);

# JFRC
use JFRC::Utils::DB qw(:all);

# ****************************************************************************
# * Global variables                                                         *
# ****************************************************************************
# Command-line parameters
my($DEBUG,$TEST,$VERBOSE) = (0)x3;

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

$log_file ||= '/opt/informatics/data/workstation_status.log';
# Display help and exit if the -help parm is specified
pod2text($0),&terminateProgram() if ($HELP);

# Initialize
$VERBOSE = 1 if ($DEBUG);
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

$sth{STATUS}->execute();
my $ar = $sth{STATUS}->fetchall_arrayref();

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
