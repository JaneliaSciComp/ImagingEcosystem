<?php

$db = mysql_pconnect('prd-db.int.janelia.org','flyportalRead','flyportalRead')
  or die('Could not connect to Workstation database: '.mysql_error());
$selected = mysql_select_db('flyportal');
if (!$selected)
  die('Unable to select flyportal database');
$type = 'mip';
if (isset($_REQUEST['type']))
  $type = ($_REQUEST['type']);
$entity = 'lsm';
if (isset($_REQUEST['entity']))
  $entity = ($_REQUEST['entity']);
$product = 'All MIP Image';
if ($entity == 'sample')
  $product = 'Default 2D Image';
if (isset($_REQUEST['product']))
  $product = $_REQUEST['product'];
$style = 'image';
if (isset($_REQUEST['style']))
  $style = ($_REQUEST['style']);
$color = '#ffffff';
if (isset($_REQUEST['color']))
  $color = ($_REQUEST['color']);
switch ($type) {
  case 'mip':
    if ($entity == 'sample') {
      sampleMIP($db,$product,$style,$color);
    }
    else {
      lsmMIP($db,$product,$style,$color);
    }
    break;
}


function linkedImage($url,$title) {
  return("<a href='#' onClick=\"openImage('"
         . $url . "','" . $title . "',1024,1024,0); "
         . "return false;\"><img src='" . $url
         . "?height=80&width=80' style='vertical-align: middle;' "
         . "height=80 width=80></a>");

}

function lsmMIP ($db,$product,$style,$color) {
  $name = preg_replace('/.*\//','',$_REQUEST['name']);
  $query = "SELECT eds.value FROM entity e JOIN entityData eds ON "
           . "(e.id=eds.parent_entity_id) WHERE e.name='" . $name
           . "' AND eds.entity_att='" . $product . "'";
  $result = mysql_query($query,$db) or die('Query failed');
  $row = mysql_fetch_array($result);
  if ($row) {
    $url = 'http://jacs-webdav.int.janelia.org/WebDAV/' . $row[0];
    echo "<img src='" . $url . "' height=200>";
  }
  else {
    echo "<div class='stamp'>No image found</div>";
  }
}


function sampleMIP ($db,$product,$style,$color) {
  $id = $_REQUEST['id'];
  $query = "SELECT e.name,edt.value,eds.value,edd.value,edsc.value,edl.value,edi.value FROM entity e JOIN entityData edt ON (e.id=edt.parent_entity_id AND edt.entity_att='TMOG Date') LEFT OUTER JOIN entityData eds ON (e.id=eds.parent_entity_id AND eds.entity_att='Status') JOIN entityData edd ON (e.id=edd.parent_entity_id AND edd.entity_att='Data Set Identifier') JOIN entityData edsc ON (e.id=edsc.parent_entity_id AND edsc.entity_att='Slide Code') JOIN entityData edl ON (e.id=edl.parent_entity_id AND edl.entity_att='Line') LEFT OUTER JOIN entityData edi ON (e.id=edi.parent_entity_id AND edi.entity_att='" . $product . "') WHERE e.id=$id";
  $result = mysql_query($query,$db) or die('Query failed');
  $row = mysql_fetch_array($result);
  $img = '';
  if ($row[6]) {
    $url = 'http://jacs-webdav.int.janelia.org/WebDAV/' . $row[6];
    $img = "<img src='" . $url . "' height=200>";
  }
  else {
    $img = "<div class='stamp'>No image found</div>";
  }
  if ($style == 'card') {
    $t = "<table class='detail'>"
         . "<tr><th>Sample name</th><td>" . $row[0] . "</td></tr>"
         . "<tr><th>TMOG date</th><td>" . $row[1] . "</td></tr>"
         . "<tr><th>Status</th><td>" . $row[2] . "</td></tr>"
         . "<tr><th>Data set</th><td>" . $row[3] . "</td></tr>"
         . "<tr><th>Slide code</th><td>" . $row[4] . "</td></tr>"
         . "<tr><th>Line</th><td>" . $row[5] . "</td></tr>"
         . "</table>";
    $d =  "<div style='float: left; border: 2px solid " . $color . "'><div style='float:left'>"
          . $img . "</div>" . "<div style='float: left;'>" . $t . "</div></div><div style='clear: both;'></div>";
    echo $d;
  }
  else {
    echo $img;
  }
}
?>
