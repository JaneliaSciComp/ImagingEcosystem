#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use Date::Parse;
use Getopt::Long;
use JSON;
use IO::File;
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
my $SESSION = 'http://informatics-flask.int.janelia.org:83/sage_responder/session';

# ****************************************************************************
# * Variables                                                                *
# ****************************************************************************
my($DEBUG,$VERBOSE,$WRITE) = (0)x3;
my $error_handle;


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************
sub initializeProgram
{
  my $filename = time . '_gen1_autoselect_errors.txt';
  $error_handle = new IO::File ">$filename";
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
      print "  Line $sl is already annotated\n" if ($VERBOSE);
      next;
    }
    # Choose a brain line/image
    if (scalar @{$line{$sl}{Brain}} == 1) {
      # Only one brain to choose from
      my $line = $line{$sl}{Brain}[0]{line};
      my $image = $line{$sl}{Brain}[0]{id};
      if ($line{$sl}{Brain}[0]{vt_status} ne 'VT mismatch') {
        # It's not a mismatch
        print "  Choose brain $line $image ($line{$sl}{Brain}[0]{vt_status})\n" if ($DEBUG);
        ($error,$vi) = &chooseVNC($line{$sl}{VNC},,$line{$sl}{Brain}[0]{capture_date});
        if ($error) {
          &notify($error);
        }
        elsif ($DEBUG) {
          print "  Choose VNC $line{$sl}{VNC}[$vi]{line} $line{$sl}{VNC}[$vi]{id} ($line{$sl}{VNC}[$vi]{vt_status})\n";
        }
        if (!$error) {
          &publishSet($line{$sl}{Brain}[0],$line{$sl}{VNC}[$vi]);
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
        print "  Choose multibrain $line $image ($line{$sl}{Brain}[$brain_index]{vt_status})\n" if ($DEBUG);
        ($error,$vi) = &chooseVNC($line{$sl}{VNC},$line{$sl}{Brain}[$brain_index]{capture_date});
        if ($error) {
          &notify($error);
        }
        elsif ($DEBUG) {
          print "  Choose VNC $line{$sl}{VNC}[$vi]{line} $line{$sl}{VNC}[$vi]{id} ($line{$sl}{VNC}[$vi]{vt_status})\n";
        }
        if (!$error) {
          &publishSet($line{$sl}{Brain}[$brain_index],$line{$sl}{VNC}[$vi]);
        }
        next;
      }
      elsif ($mm eq scalar(@{$line{$sl}{Brain}})) {
        &notify("Can't use brains from $sl (all are mismatches)");
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
  my($vncarr,$brain_cd) = @_;
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
    my $brain_time = str2time($brain_cd);
    my $min_delta = 1e9;
    my $vnc_index = -1;
    foreach (0..scalar(@$vncarr)-1) {
      next if ($vncarr->[$_]{vt_status} eq 'VT mismatch');
      return(0,$_) unless ($brain_cd);
      my $vnc_time = str2time($vncarr->[$_]{capture_date});
      my $delta = abs($vnc_time - $brain_time);
      print "  Compare times $vncarr->[$_]{id} ($vncarr->[$_]{capture_date} $vnc_time) to $brain_time: $delta\n"
      if ($DEBUG);
      if ($delta < $min_delta) {
        $min_delta = $delta;
        $vnc_index = $_;
      }
    }
    if ($vnc_index == -1) {
      return("Can't find VNC with good VT status",0);
    }
    else {
      return(0,$vnc_index);
    }
  }
}


sub publishSet
{
  my($brain,$vnc) = @_;
  if ($VERBOSE) {
    printf "  Publish Brain $brain->{line},$brain->{name}\n";
    printf "          VNC $vnc->{line},$vnc->{name}\n";
  }
  return unless ($WRITE);
  my $msg = &publish($brain);
  if ($msg) {
    &notify($msg);
    return();
  }
  $msg = &publish($vnc);
  &notify($msg) if ($msg);
}

sub publish
{
  my($info) = @_;
  my $rest = $CONFIG{sage}{url}.'lines?name=' . $info->{line} . '&_columns=id';
  my $response = get $rest;
  &terminateProgram("REST GET returned null response (Request: $rest)")
    unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  return("Could not find ID for $info->{line}") unless ($rvar->{line_data}[0]{id});
  my $ua = LWP::UserAgent->new;
  my $req = HTTP::Request->new(POST => $SESSION);
  $req->header('content-type' => 'application/json');
  $req->header('access-control-allow-origin' => '*');
  my $post_data = {cv => "flylight_public_annotation",
                   type => "annotation_image",
                   name => $info->{name},
                   line_id => $rvar->{line_data}[0]{id},
                   image_id => $info->{id},
                   annotator => "svirskasr",
                   lab => "flylight"};
  my $json = JSON->new->allow_nonref;
  $req->content($json->encode($post_data));
  my $resp = $ua->request($req);
  unless ($resp->{_rc} eq 200) {
    return($resp->{_msg});
  }
  return();
}


sub notify
{
  my($msg) = shift;
  print "  $msg\n";
  print $error_handle "$msg\n";
}


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
GetOptions(write        => \$WRITE,
           verbose      => \$VERBOSE,
           debug        => \$DEBUG);
&initializeProgram();
&processLines();
$error_handle->close();
