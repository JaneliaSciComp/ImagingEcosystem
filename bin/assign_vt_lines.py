#!/opt/python/bin/python2.7

import argparse
import sys
import MySQLdb

# SQL statements
SQL = {'LINES': "SELECT l.id,l.name,lp.value FROM line l JOIN " +
       "line_property_vw lp ON (l.id=lp.line_id AND lp.type='fragment')" +
       " WHERE l.name LIKE 'BJD_1%' and l.name not in (SELECT DISTINCT name " +
       "FROM line_property_vw WHERE type='vt_line' AND name LIKE 'BJD_1%') " +
       "ORDER BY 2",
       'IMAGES': "SELECT line,i.id,value FROM image_data_mv i " +
       "JOIN line_property_vw l ON (line=l.name AND type='vt_line') WHERE " +
       "line LIKE 'BJD_1%' AND vt_line IS NULL ORDER BY 1,2",
       'INSERT_LP': "INSERT INTO line_property (line_id,type_id,value) " +
       "VALUES (%s,getCvTermId('light_imagery','vt_line',''),%s)",
       'INSERT_IP': "INSERT INTO image_property (image_id,type_id,value) " +
       "VALUES (%s,getCvTermId('light_imagery','vt_line',''),%s)",
       }


def sqlError(e):
    try:
        print 'MySQL error [%d]: %s' % (e.args[0], e.args[1])
    except IndexError:
        print 'MySQL error: %s' % e
    sys.exit(-1)


def dbConnect():
    try:
        conn = MySQLdb.connect(host='mysql3', user='sageApp',
                               passwd='h3ll0K1tty', db='sage')
    except MySQLdb.Error as e:
        sqlError(e)
    try:
        cursor = conn.cursor()
        return(conn, cursor)
    except MySQLdb.Error as e:
        sqlError(e)


def findLines(conn, cursor):
    try:
        cursor.execute(SQL['LINES'])
    except MySQLdb.Error as e:
        sqlError(e)
    lcount = 0
    for (id, line, vt) in cursor:
        if (VERBOSE):
            print "%d\t%s\t%s" % (int(id), line, vt)
        try:
            cursor.execute(SQL['INSERT_LP'], [id, vt])
            lcount += 1
        except MySQLdb.Error, e:
            sqlError(e)
        if (WRITE and cursor.rowcount == 1):
            conn.commit()
    try:
        cursor.execute(SQL['IMAGES'])
    except MySQLdb.Error as e:
        sqlError(e)
    icount = 0
    for (line, id, vt) in cursor:
        if (VERBOSE):
            print "%s\t%d\t%s" % (line, int(id), vt)
        try:
            cursor.execute(SQL['INSERT_IP'], [id, vt])
            icount += 1
        except MySQLdb.Error, e:
            sqlError(e)
        if (WRITE and cursor.rowcount == 1):
            conn.commit()
    print "Number of lines fixed: %d" % (lcount)
    print "Number of images fixed: %d" % (icount)

# -----------------------------------------------------------------------------


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Add VT line to Dickson Vienna (BJD) lines')
    parser.add_argument('-verbose', action='store_true', dest='verbose',
                        default=False, help='Turn on verbose output')
    parser.add_argument('-write', action='store_true', dest='write',
                        default=False, help='Write to database')
    args = parser.parse_args()
    VERBOSE = args.verbose
    WRITE = args.write
    (conn, cursor) = dbConnect()
    findLines(conn, cursor)
    sys.exit(0)
