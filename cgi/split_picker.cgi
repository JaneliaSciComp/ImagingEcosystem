#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Session;
use DBI;
use IO::File;
use LWP::UserAgent;
use HTTP::Request;
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Split Picker';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
my $URL = 'http://informatics-prod.int.janelia.org/cgi-bin/view_sage_imagery.cgi'
            . '?_op=images&_family=dickson&_header=0&_size=';
my $CBASE = 'http://img.int.janelia.org/flylight-image/dickson-vienna-confocal-data/';
my $PBASE = 'http://img.int.janelia.org/flylight-image/dickson-vienna-secondary-data/projections/';
my $SIZE = 500;
my $TSIZE = 150;
use constant NBSP => '&nbsp;';
use constant USER => 'sageRead';
my $DB = 'dbi:mysql:dbname=sage;host=';

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************

# Database
my $DATBASE;
my %sth = (
CROSSES => 'SELECT name FROM line WHERE name LIKE ?',
LIST => "SELECT DISTINCT line FROM image_data_mv WHERE family='rubin_chacrm' "
        . "ORDER BY 1",
CONVERT => 'SELECT DISTINCT vt_line FROM image_data_mv WHERE line=?',
LINE => "SELECT s.url,i.url,id.driver,organ,gender,id.line,'?' FROM secondary_image_vw s JOIN image_data_mv id "
        . "ON (id.id=s.image_id) JOIN image i on (i.id=id.id) WHERE id.line=? "
        . "AND s.product='projection_all' AND id.family IN ('dickson','rubin_chacrm')",
VT => "SELECT s.url,i.url,id.driver,organ,gender,id.vt_line,lp.value AS landing_site FROM secondary_image_vw s JOIN image_data_mv id "
      . "ON (id.id=s.image_id) JOIN image i on (i.id=id.id) LEFT OUTER JOIN line_property_vw lp ON (lp.name=id.line AND lp.cv='line' AND lp.type='flycore_landing_site') WHERE id.vt_line=? "
      . "AND s.product='projection_all'",
);
my @impdata;



# Session authentication
my $Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
&terminateProgram('You are not authorized to run the Split Picker')
  unless ($Session->param('rubin_imagery'));
my $USERID = $Session->param('user_id');

# Connect to database
my $DATABASE = lc(param('_database') || 'prod');
$DB .= ($DATABASE eq 'prod') ? 'mysql3' : 'db-dev';
my $dbh = DBI->connect($DB,(USER)x2,{RaiseError=>1,PrintError=>0});
$sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
  foreach (keys %sth);

if (param('line')) {
  &displayLine(uc(param('line')));
}
else {
  &getLine();
}
exit(0);


sub getLine
{
  $sth{LIST}->execute();
  my $ar = $sth{LIST}->fetchall_arrayref();
  my @line_list = map {$_->[0]} @$ar;
  my $ifile = '/groups/flylight/flylight/dickson/Vienna/confocal_index.txt';
  $ifile = '/groups/flylight/flylight/VT_Image_Data/line_index.txt';
  open FILE,$ifile
    or &terminateProgram("Could not open $ifile: $!");
  while (<FILE>) {
    chomp;
    s/.*\///;
    push @line_list,$_;
  }
  close(FILE);
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    ('chosen.jquery.min',$PROGRAM);
  my @styles = map { Link({-rel=>'stylesheet',
                           -type=>'text/css',-href=>"/css/$_.css"}) }
                   qw(chosen.min);
  print &standardHeader(title => $APPLICATION,
                        css_prefix => $PROGRAM,
                        script => \@scripts,
                        style => \@styles,
                        breadcrumbs => \@BREADCRUMBS,
                        expires => 'now'),
        start_multipart_form;
  print div({class => 'boxed'},
            div({align => 'center'},h2('Split line selection')),br,
            div({class => 'input_block'},
                h3('Reference line',span({style => 'font-size: 9pt'},'(optional)')),
                table(Tr(td(['Line: ',
                             popup_menu(&identify('line'),
                                        'data-placeholder' => 'Choose a line..',
                                        -class => 'chosen-select',
                                        -values => ['NONE',sort @line_list])
                            ])),
                      &generalSearches('template'),
                     ),
               ),
            div({class => 'input_block'},
                h3('Search lines'),
                table(Tr(td(['File containing search set lines:',
                             filefield({&identify('line_file'),size=>40})])),
                      &generalSearches('target'),
                     ),
               ),
            div({style => 'clear: both;'},NBSP),
            'Imagery type given priority for IMP images: ',
            popup_menu(&identify('priority'),
                       -values => ['Aligned MIP','Original MIP (Z projection)']),
            br,
            'Thumbnail image size: ',
            input({&identify('tsize'),
                   size => 3,
                   value => $TSIZE}),
            br,
            div({align => 'center'},
                submit({class => 'submitbutton',value => 'Search'})),br,
            div({class => 'alert alert-warning',role => 'alert'},
                span({class => 'glyphicon glyphicon-warning-sign'},''),
                'Note that all ',
                a({href => 'http://www.imp.ac.at/research/research-groups/dickson-group/research',
                   target => '_blank'},'IMP'),
                'images are male brains'),
           );
  print end_form,&sessionFooter($Session),end_html;
}


sub generalSearches
{
  my($which) = shift;
  return(Tr(td(['Driver: ',
                popup_menu(&identify($which.'_driver'),
                           -values => [qw(Any GAL4 LexA LexAGAD LexAp65 p65ADZp ZpGAL4DBD)])])) .
         Tr(td(['Area: ',
                popup_menu(&identify($which.'_area'),
                           -values => ['Any','Brain','Ventral Nerve Cord'])])) .
         Tr(td(['Gender: ',
                popup_menu(&identify($which.'_gender'),
                           -values => [qw(Any Female Male)])])) .
         Tr(td(['Image source: ',
                popup_menu(&identify($which.'_source'),
                           -values => [qw(Any Janelia IMP)])]))
  );
}


sub displayLine
{
  my($line) = shift;
  my $no_reference = ($line eq 'NONE') ? 1 : 0;
  my $area = param('template_area');
  my $driver = param('template_driver');
  my $gender = param('template_gender');
  my $source = param('template_source');
  # Convert BJD line to VT line
  if ($line =~ /^BJD/) {
    $sth{CONVERT}->execute($line);
    my($vt) = $sth{CONVERT}->fetchrow_array;
    $line = $vt if ($vt);
  }
  my (@confocal,@line,@projection);
  my $reference_block;
  if ($no_reference) {
    $reference_block = 
      div({&identify('crossarea')},
          h2('Selected lines:'),br,
          div({&identify('crosses')},NBSP));
  }
  else {
    # Reference (Template)
    # IMP images are all male brains
    if (($line =~ /^VT/) && ($source ne 'Janelia') && ($area ne 'Ventral Nerve Cord') && ($gender ne 'Female')) {
      my($num) = $line =~ /(\d+)/;
      $line = sprintf 'VT%06d',$num;
      &gatherIMPImagery($line,$driver,\@confocal,\@projection,\@line);
    }
    if ($source ne 'IMP') {
      my $s = ($line =~ /^VT/) ? 'VT' : 'LINE';
      $sth{$s}->execute($line);
      my $ar = $sth{$s}->fetchall_arrayref();
      foreach (@$ar) {
        my $dd = $_->[2];
        $dd = 'GAL4' if ($dd eq 'GAL4_Collection');
        my $aa = $_->[3];
        my $gg = $_->[4];
        if (&useImage($aa,$area,$dd,$driver,$gg,$gender)) {
          push @projection,$_->[0];
          push @confocal,$_->[1];
          if ($line =~ /^VT/) {
            $_->[6] ||= 'unknown';
            push @line,"$_->[5]-$dd@" . $_->[6];
          }
          else {
            push @line,$_->[5] . ' ' . $dd;
          }
        }
      }
    }
    my $ic1 = scalar(@confocal);
    my $primary = &primaryImageSection($line,$driver,$area,\@confocal,\@projection,\@line);
    $reference_block =
      div({&identify('left_block')},
          div({&identify('activation_domain')},
          h1('Reference'),
          div({&identify('primary')},
              div({style => 'width: 33%; float: left;'},
                  'Images:',$ic1),
              div({style => 'width: 33%; float: left;'},
                  table(Tr(td(['Driver:',param('template_driver')])),
                        Tr(td(['Area:',param('template_area')])))),
              div({style => 'width: 33%; float: left;'},
                  table(Tr(td(['Gender:',param('template_gender')])),
                        Tr(td(['Image source:',param('template_source')])))),
              br,$primary)),
          br,
          div({&identify('crossarea')},
              h2('Selected lines:'),br,
              div({&identify('crosses')},NBSP)));
  }
  # Search (Target)
  my $search = '';
  $area = param('target_area');
  $driver = param('target_driver');
  $gender = param('target_gender');
  $source = param('target_source');
  my($lc,$ic,$dbdhtml) = &dbdImageSection($area,$driver,$gender,$source);
  my $search_block =
    div({&identify(($no_reference) ? 'bottom_block' : 'right_block')},
        h1('Search'),
        div({style => 'width: 33%; float: left;'},
            table(Tr(td(['Lines:',$lc])),
                  Tr(td(['Images:',$ic])))),
        div({style => 'width: 33%; float: left;'},
            table(Tr(td(['Driver:',param('target_driver')])),
                  Tr(td(['Area:',param('target_area')])))),
        div({style => 'width: 33%; float: left;'},
            table(Tr(td(['Gender:',param('target_gender')])),
                  Tr(td(['Image source:',param('target_source')])))),
        br, 
        button(-value => 'Show all images',
               -class => 'smallbutton',
               -onclick => 'showAll();'),
        button(-value => 'Hide unchecked images',
               -class => 'smallbutton',
               -onclick => 'hideUnchecked();'),
        button(-value => 'Hide checked images',
               -class => 'smallbutton',
               -onclick => 'hideChecked();'),
        div({&identify('scrollarea')},$dbdhtml),);
  &printHeader();
  print start_form,
        div({class => 'boxed',
             style => 'margin: 0 10px 0 10px; min-height: 930px;'},
            $reference_block,$search_block,
            div({style => 'clear: both;'},NBSP),
           );
  if (0) {
    &printHeader();
    my $error = <<__EOT__;
No images were found for $line<br>
Driver: $driver<br>
Area: $area<br>
Gender: $gender<br>
Image source: $source<br>
__EOT__
    print p({class => "bg-danger"},$error);
  }
  print div({style => 'clear: both;'},NBSP),
        end_form,&sessionFooter($Session),end_html;
}


sub printHeader {
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    ($PROGRAM);
  print &standardHeader(title => $APPLICATION,
                        css_prefix => $PROGRAM,
                        script => \@scripts,
                        breadcrumbs => \@BREADCRUMBS,
                        expires => 'now');
}


sub primaryImageSection
{
  my($line,$driver,$area,$confocal,$projection,$lineref) = @_;
  # Carousel
  my $html = div({align => 'center'},
                 h2({style => 'color: cyan'},div({class => 'line',id => $line},$line)))
             . '<div id="carousel-example-generic" class="carousel slide" '
             . 'data-ride="carousel" data-interval="0">';
  # Carousel inner
  $html .= '<div class="carousel-inner" role="listbox" style="align: center">';
  my $cclass = 'item active';
  foreach my $stack (@$confocal) {
    my($proj) = shift(@$projection);
    (my $caption = $stack) =~ s/.*\///;
    $caption =~ s/.*%2F//;
    $caption =~ s/\.(?:am|lsm).*//;
    # Replace caption with line/driver
    $caption = shift  @$lineref;
    (my $title = $proj) =~ s/.*\///;
    $title =~ s/\.(?:jpg|png)//;
    my $url = ($proj =~ /^http/) ? $proj : "$PBASE/$proj";
    my $url2 = ($stack =~ /^http/) ? $stack : "$CBASE/$stack";
    my($iw,$ih) = (1024)x2;
    $html .= div({class => $cclass},
                 a({href    => '#',
                    onClick => "openImage('$url','$title',$iw,$ih);"
                               . " return false;"},
                 img({src => "$url?width=$SIZE&proportional=1"})),
                 div({class => 'carousel-caption'},
                     a({href => $url2,
                        style => 'color: cyan'},$caption)));
    $cclass = 'item';
  }
  # Carousel navigation
  $html .= <<__EOT__;
</div>
<a class="left carousel-control" href="#carousel-example-generic" role="button" data-slide="prev">
  <span class="glyphicon glyphicon-chevron-left" aria-hidden="true"></span>
  <span class="sr-only">Previous</span>
</a>
<a class="right carousel-control" href="#carousel-example-generic" role="button" data-slide="next">
  <span class="glyphicon glyphicon-chevron-right" aria-hidden="true"></span>
  <span class="sr-only">Next</span>
</a>
__EOT__
  # Carousel indicators
  $html .= '<ol class="carousel-indicators">';
  foreach my $i (0..$#$confocal) {
    $html .= '<li data-target="#carousel-example-generic" data-slide-to="'
              . $i . '"' . (($i) ? '' : ' class="active"')
              . "></li>\n";
  }
  $html .= "</ol>\n";
  $html .= '</div>';
  return(div({&identify('primary_carousel')},$html));
}


sub dbdImageSection
{
  my($area,$driver,$gender,$source) = @_;
  my $input_file = param('line_file');
  $TSIZE = param('tsize');
  my @full_line_list = &convert_input($input_file);
  my (@line_list,@vt_line_list);
  foreach (@full_line_list) {
    (/^VT/) ? push @vt_line_list,$_ : push @line_list,$_;
  }
  my (@confocal,@line,@projection);
  if ($source ne 'IMP') {
    my @ar;
    my $sql;
    if (scalar @line_list) {
      $sql = "SELECT s.url,i.url,id.driver,organ,gender,id.line FROM "
             . "secondary_image_vw s JOIN image_data_mv id ON "
             . "(id.id=s.image_id) JOIN image i on (i.id=id.id) WHERE "
             . "id.line IN (" . join(',',map {"'$_'"} @line_list)
             . ") AND s.product='projection_all' AND id.family IN "
             . " ('dickson','rubin_chacrm','flylight_polarity') "
             . "AND s.url IS NOT NULL ORDER BY id.line ASC,id.representative "
             . "DESC";
      print STDERR "$sql\n";
      my $ar1 = $dbh->selectall_arrayref($sql);
      push @ar,@$ar1;
    }
    if (scalar @vt_line_list) {
      $sql = "SELECT s.url,i.url,id.driver,organ,gender,id.vt_line,lp.value AS landing_site FROM "
             . "secondary_image_vw s JOIN image_data_mv id ON "
             . "(id.id=s.image_id) JOIN image i on (i.id=id.id) LEFT OUTER JOIN line_property_vw lp ON (lp.name=id.line AND lp.cv='line' AND lp.type='flycore_landing_site') WHERE "
             . "id.vt_line IN (" . join(',',map {"'$_'"} @vt_line_list)
             . ") AND s.product='projection_all' AND id.family='dickson' "
             . "AND s.url IS NOT NULL ORDER BY id.line ASC,id.representative "
             . "DESC";
      print STDERR "$sql\n";
      my $ar1 = $dbh->selectall_arrayref($sql);
      push @ar,@$ar1;
    }
    foreach (@ar) {
      my $dd = $_->[2];
      $dd = 'GAL4' if ($dd eq 'GAL4_Collection');
      my $aa = $_->[3];
      my $gg = $_->[4];
      if (&useImage($aa,$area,$dd,$driver,$gg,$gender)) {
        push @projection,$_->[0];
        push @confocal,$_->[1];
        if ($_->[5] =~ /^VT/) {
          $_->[6] ||= 'unknown';
          push @line,"$_->[5]-$dd@" . $_->[6];
        }
        else {
          push @line,$_->[5] . ' ' . $dd;
        }
      }
    }
  }
  # IMP images are all male brains
  if (($source ne 'Janelia') && ($area ne 'Ventral Nerve Cord') && ($gender ne 'Female')) {
    my(@impc,@impp);
    foreach (@vt_line_list) {
      &gatherIMPImagery($_,$driver,\@impc,\@impp,\@line)
        if (/^VT/);
    }
    push @confocal,@impc;
    push @projection,@impp;
  }
  my $html = '';
  my %linehash;
  my $imagenum = 1;
  foreach my $stack (@confocal) {
    my($proj) = shift(@projection);
    my($this_line) = shift(@line);
    (my $this_line_id = $this_line) =~ s/ .+//;
    $linehash{$this_line_id}++;
    (my $caption = $stack) =~ s/.*\///;
    $caption =~ s/.*%2F//;
    $caption =~ s/\.lsm.*//;
    (my $title = $proj) =~ s/.*\///;
    $title =~ s/\.jpg//;
    my $url = ($proj =~ /^http/) ? $proj : "$PBASE/$proj";
    my $url2 = ($stack =~ /^http/) ? $stack : "$CBASE/$stack";
    my($iw,$ih) = (1024)x2;
    my %parms = ($url =~ /(?:webdav|jade)/i) ? (width => $TSIZE, height => $TSIZE) : ();
    $html .= div({&identify('img'.$imagenum),class => 'single_dbd'},
                 a({href    => '#',
                    onClick => "openImage('$url','$title',$iw,$ih);"
                               . " return false;"},
                 img({src => "$url?width=$TSIZE&proportional=1",%parms})),
                 div({&identify('cpt'.$imagenum)},
                     div({&identify('cb'.$imagenum++),
                          class => 'checkbox'},
                         input({type => 'checkbox',
                                id => $this_line_id,
                                class => 'lineselect',
                                onclick => 'refreshCrosses();'},
                               a({href => $url2,
                                  style => 'color:cyan;font-size:9pt;'},
                                 $this_line)
                              )),
                    ));
  }
  return(scalar(keys %linehash),scalar(@confocal),$html);
}


# ****************************************************************************
# * Subroutine:  convert_input                                               *
# * Description: This routine will create an array of rows from the input    *
# *              stream. This may seem like a waste of time, but trust me:   *
# *              it ain't. We should be able to handle any common IFS, and   *
# *              the expected input will be from Excel running on a Mac.     *
# *              Despite the fact that Excel is running on OS X (Unix-style  *
# *              line separators), it still produces the old-school Mac OFS. *
# *                                                                          *
# * Parameters:  stream: file handle to process                              *
# * Returns:     @field: array of input rows                                 *
# ****************************************************************************
 sub convert_input
 {
  my $stream = shift;
  my $buffer = '';
  $buffer .= $_ while (<$stream>);
  # Windows: CR/LF
  my @field = split(/\015\012/,$buffer);
  &terminateProgram('Could not process import file') unless (scalar @field);
  if (1 == scalar(@field)) {
     # Mac (pre-OS X): CR
     @field = split(/\012/,$buffer);
     if (1 == scalar(@field)) {
       # Unix: LF
       @field = split(/\015/,$buffer);
       if (1 == scalar(@field)) {
         &terminateProgram('Unknown input format');
       }
     }
   }
   return(@field);
}


sub gatherIMPImagery
{
  my($line,$driver,$confocal,$projection,$lineref) = @_;
  unless (scalar @impdata) {
    my $file = '/groups/flylight/flylight/VT_Image_Data/master_index';
    if (param('priority') =~ /Aligned/) {
      $file .= '_mip' if (param('priority') =~ /MIP/);
    }
    else {
      $file .= '_proj' unless (param('priority') =~ /preview/);
    }
    $file .= '.txt';
    my $fh = new IO::File "<$file";
    while (<$fh>) {
      chomp;
      push @impdata,[split(/\t/)];
    }
    $fh->close;
  }
  my $found = 0;
  foreach (@impdata) {
    if ($line eq $_->[0]) {
      $found++;
      if (&useImage('Brain',param('target_area'),$_->[1],$driver,'Male',param('target_gender'))) {
        push @$confocal,$_->[3];
        push @$projection,$_->[2];
        (my $ls = $_->[3]) =~ s/\_[^_]+.(?:am|lsm)$//;
        $ls =~ s/.+_//;
        push @$lineref,"$line-$_->[1]@" . $ls if ($lineref);
      }
    }
    else {
      last if ($found);
    }
  }
}


sub useImage
{
  my($image_area,$check_area,$image_driver,$check_driver,$image_gender,$check_gender) = @_;
  # Process area
  if (!(($image_area =~ /^V/) && ($check_area =~ /^V/))) {
    return(0) if (($image_area ne $check_area) && ($check_area ne 'Any'));
  }
  # Process gender
  if (($image_gender ne 'unknown') && ($check_gender ne 'Any')) {
    return(0) if ($image_gender ne $check_gender);
  }
  # Process driver
  if ($image_driver && ($check_driver !~ /^(?:LexA|Any)$/)) {
    return ($check_driver eq $image_driver) ? 1 : 0;
  }
  return ((($check_driver eq 'LexA') && ($image_driver =~ /lexa/i))
      || (($check_driver eq 'GAL4') && ($image_driver !~ /lexa/i))
      || (!$image_driver) || ($check_driver eq 'Any')) ? 1 : 0;
}
