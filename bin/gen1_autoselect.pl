#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use JSON;
use LWP::Simple;
use POSIX qw(ceil);
use Switch;

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/opt/informatics/data/';

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
my %CONFIG;
my($DEBUG,$VERBOSE,$WRITE) = (0)x3;


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************
sub initializeProgram
{
  # Get WS REST config
  my $file = DATA_PATH . 'rest_services.json';
  open SLURP,$file or &terminateProgram("Can't open $file: $!");
  sysread SLURP,my $slurp,-s SLURP;
  close(SLURP);
  my $hr = decode_json $slurp;
  %CONFIG = %$hr;
}


sub processLines
{
  my $rest = $CONFIG{sage}{url}.'gen1_images?family=dickson&line=BJD_1*';
  my $response = get $rest;
  &terminateProgram("REST GET returned null response (Request: $rest)")
    unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  my (%line,%line_count,%pie,%pw);
  my $selected = 0;
  foreach (@{$rvar->{image_data}}) {
    my($line,$driver,$area,$annotator,$vtid,$vtmatch) = @$_{qw(line driver area annotator vt_id_found vt_library_match)};
    next if ($driver ne 'GAL4_Collection');
    my($short) = $line =~ /(BJD_\d{3}.{3})/;
    my $vt;
    if ($vtmatch eq 'Y') {
      $vt = 'VT match';
    }
    elsif ($vtid eq 'Y') {
      $vt = 'VT mismatch';
    }
    else {
      $vt = 'No VT';
    }
    $_->{vt_status} = $vt;
    push @{$line{$short}{$area}},$_;
    $annotator ||= '';
    $line{$short}{Annotated}{$area}++ if ($annotator);
  }
  foreach my $sl (sort keys %line) {
    print "$sl\n";
    my($error,$vi) = (0)x2;
    # Skip if we don't have a brain or VNC
    foreach (qw(Brain VNC)) {
      unless (exists $line{$sl}{$_}) {
        &notify("Missing $_ for $sl");
        $error++;
      }
    }
    next if ($error);
    # Skip annotated lines
    if ($line{$sl}{Annotated}{Brain} && $line{$sl}{Annotated}{VNC}) {
      print "  Line $sl is already annotated\n";
      next;
    }
    # Choose a brain line/image
    if (scalar @{$line{$sl}{Brain}} == 1) {
      # Only one brain to choose from
      my $line = $line{$sl}{Brain}[0]{line};
      my $image = $line{$sl}{Brain}[0]{id};
      if ($line{$sl}{Brain}[0]{vt_status} ne 'VT mismatch') {
        # It's not a mismatch
        print "  Choose brain $line $image ($line{$sl}{Brain}[0]{vt_status})\n";
        ($error,$vi) = &chooseVNC($line{$sl}{VNC},$line{$sl}{Brain}[0]{id});
        ($error) ? &notify($error) : print "  Choose VNC $line{$sl}{VNC}[$vi]{line} $line{$sl}{VNC}[$vi]{id} ($line{$sl}{VNC}[$vi]{vt_status})\n";
        if (!$error) {
          print "  Publish $line/$image $line{$sl}{VNC}[$vi]{line}/$line{$sl}{VNC}[$vi]{id}\n";
        }
        next;
      }
      else {
        # It's a mismatch
        &notify("Can't use brain $line $image ($line{$sl}{Brain}[0]{vt_status})");
        next;
      }
    }
    else {
      # More than one brain
      my $max_qi = 0;
      my $brain_index = -1;
      my $mm = 0;
      my ($image,$line);
      foreach (0..scalar(@{$line{$sl}{Brain}})-1) {
        $line = $line{$sl}{Brain}[$_]{line};
        $image = $line{$sl}{Brain}[$_]{id};
        my $qi = $line{$sl}{Brain}[$_]{qi} || 0;
        if (($line{$sl}{Brain}[$_]{vt_status} ne 'VT mismatch')
            && ($qi > $max_qi)) {
          $max_qi = $qi;
          $brain_index = $_;
        }
        elsif ($line{$sl}{Brain}[$_]{vt_status} eq 'VT mismatch') {
          $mm++;
        }
      }
      if ($max_qi) {
        print "  Choose multibrain $line $image ($line{$sl}{Brain}[$brain_index]{vt_status})\n";
        ($error,$vi) = &chooseVNC($line{$sl}{VNC},$line{$sl}{Brain}[$brain_index]{id});
        ($error) ? &notify($error) : print "  Choose VNC $line{$sl}{VNC}[$vi]{line} $line{$sl}{VNC}[$vi]{id} ($line{$sl}{VNC}[$vi]{vt_status})\n";
        if (!$error) {
          print "  Publish $line/$image $line{$sl}{VNC}[$vi]{line}/$line{$sl}{VNC}[$vi]{id}\n";
        }
        next;
      }
      elsif ($mm eq scalar(@{$line{$sl}{Brain}})) {
        &notify("All brains are VT mismatches for $sl");
        next;
      }
      else {
        &notify("Could not find brains with Qi scores for $sl");
        next;
      }
    }
    print "  Dead end for $sl\n";
  }
}


sub chooseVNC
{
  my($vncarr,$brain_id) = @_;
  if (scalar(@$vncarr) == 1) {
    # One VNC
    my $line = $vncarr->[0]{line};
    my $image = $vncarr->[0]{id};
    if ($vncarr->[0]{vt_status} ne 'VT mismatch') {
      return((0)x2);
    }
    else {
      return("Can't use VNC $line $image ($vncarr->[0]{vt_status})",0);
    }
  }
  else {
    # Multiple VNCs
    foreach (0..scalar(@$vncarr)-1) {
      return(0,$_) unless ($vncarr->[$_]{vt_status} eq 'VT mismatch');
    }
    return("Can't find VNC with good VT status",0);
  }
}


sub notify
{
  my($msg) = shift;
  print "  $msg\n";
}


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
&processLines();
