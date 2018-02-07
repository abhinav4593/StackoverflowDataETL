#Creates tables from xml data dumps - Scans all xml files in subdirectories

import os
#import fnmatch
import re
import sys
from datetime import datetime
import logging
#from StringIO import StringIO

from lxml import etree

#sqlalchemy imports
import sqlalchemy as sa
#from sqlalchemy.dialects.postgresql import ARRAY, BIGINT, BIT, BOOLEAN, BYTEA, CHAR, CIDR, DATE,DOUBLE_PRECISION, ENUM, FLOAT, HSTORE, INET, INTEGER, INTERVAL, MACADDR, NUMERIC, REAL, SMALLINT, TEXT, TIME, TIMESTAMP, UUID, VARCHAR
from sqlalchemy.dialects.postgresql import INTEGER,TIMESTAMP,VARCHAR,SMALLINT,UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import create_session
from sqlalchemy import create_engine,Column

#constants
#DATADIR='E:\\IA_Work\\Abhinav\\Modification\\data'
DATADIR=sys.argv[1]
RE_XML_ILLEGAL = u'([\u0000-\u0008\u000b-\u000c\u000e-\u001f\ufffe-\uffff])' + u'|' + u'([%s-%s][^%s-%s])|([^%s-%s][%s-%s])|([%s-%s]$)|(^[%s-%s])' % (unichr(0xd800),unichr(0xdbff),unichr(0xdc00),unichr(0xdfff),unichr(0xd800),unichr(0xdbff),unichr(0xdc00),unichr(0xdfff),unichr(0xd800),unichr(0xdbff),unichr(0xdc00),unichr(0xdfff))
ILLEGAL_UNICHRS = [ (0x00, 0x08), (0x0B, 0x1F), (0x7F, 0x84), (0x86, 0x9F),
                    (0xD800, 0xDFFF), (0xFDD0, 0xFDDF), (0xFFFE, 0xFFFF),
                    (0x1FFFE, 0x1FFFF), (0x2FFFE, 0x2FFFF), (0x3FFFE, 0x3FFFF),
                    (0x4FFFE, 0x4FFFF), (0x5FFFE, 0x5FFFF), (0x6FFFE, 0x6FFFF),
                    (0x7FFFE, 0x7FFFF), (0x8FFFE, 0x8FFFF), (0x9FFFE, 0x9FFFF),
                    (0xAFFFE, 0xAFFFF), (0xBFFFE, 0xBFFFF), (0xCFFFE, 0xCFFFF),
                    (0xDFFFE, 0xDFFFF), (0xEFFFE, 0xEFFFF), (0xFFFFE, 0xFFFFF),
                    (0x10FFFE, 0x10FFFF) ]

###clean XML for illegal characters in XML (the XMLS are in UTF-8)
illegal_ranges = ["%s-%s" % (unichr(low), unichr(high))
                  for (low, high) in ILLEGAL_UNICHRS
                  if low < sys.maxunicode]
illegal_xml_re_str = u'[%s]' % u''.join(illegal_ranges)+RE_XML_ILLEGAL
illegal_xml_re = re.compile(illegal_xml_re_str)


#postgresql connection
dbstr=sys.argv[2]
engine = create_engine('postgresql+psycopg2://XXXX:***********@localhost:5435/'+dbstr, echo=False, client_encoding='utf8')
metadata = sa.MetaData()
Base = declarative_base(bind=engine)
conn = engine.connect()


#LOGGING THE SESSION
now = datetime.now()
now1 = now.strftime("%Y-%m-%d %H:%M")
now2 = now1.replace(" ", '_')
datestr=now2
log_file_name = '../log/' 'logging_'+datestr
logger = logging.getLogger('stack')
logger.setLevel(logging.INFO)
# create file handler which log even debug messages
fh = logging.FileHandler(log_file_name)
fh.setLevel(logging.INFO)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(ch)
logger.addHandler(fh)
logger2 = logging.getLogger('stack_files_completed')
logger2.setLevel(logging.INFO)
logger2.addHandler(logging.FileHandler('../log/completedFiles.txt'))


def validate(l):
	if (l.viewcount=='' or l.viewcount==' '):
                l.viewcount=-9999
	return l

'''
***************
Note:
The sitename and filepath are actually set not in inclass function
but in inserttodatabase function.
**************
'''

class Badges(Base):
    __tablename__ = 'badges'

    UID=Column(INTEGER, primary_key=True,autoincrement=True )
    userid=Column(INTEGER,default = "-9999")
    name=Column(VARCHAR)
    date=Column(TIMESTAMP)
    #filepath=Column(VARCHAR)
    #sitename=Column(VARCHAR)

    @classmethod
    def from_xmlgroup(cls,element):
        l= cls()
        l.userid = element.get('UserId')
        l.name = element.get('Name')
        l.date = element.get('Date')
        l.sitename = os.path.dirname(os.path.realpath(__file__))
        return l

class Posts(Base):
    __tablename__ = 'posts'

    UID=Column(INTEGER, primary_key=True,autoincrement=True )
    id = Column(INTEGER,default ="-9999")
    posttypeid = Column(SMALLINT,default ="-9999")
    parentid = Column(INTEGER, default="-9999")
    acceptedanswerid = Column(INTEGER,default="-9999")
    creationdate = Column(TIMESTAMP)
    score = Column(SMALLINT, default="-99999")
    viewcount = Column(INTEGER,default="-9999")
    body = Column(VARCHAR)
    owneruserid = Column(INTEGER,default="-9999")
    lasteditoruserid = Column(INTEGER,default="-9999")
    lasteditordisplayname = Column(VARCHAR)
    lasteditdate = Column(TIMESTAMP)
    lastactivitydate = Column(TIMESTAMP)
    communityowneddate = Column(TIMESTAMP)
    closeddate = Column(TIMESTAMP)
    title = Column(VARCHAR)
    tags = Column(VARCHAR)
    answercount = Column(SMALLINT,default="-9999")
    commentcount = Column(SMALLINT,default="-9999")
    favoritecount = Column(INTEGER,default="-9999")
    #filepath=Column(VARCHAR)
    #sitename=Column(VARCHAR)


    @classmethod
    def from_xmlgroup(cls, element):
        l = cls() #used as the first ar.guments to class arguments
        l.id = element.get('Id')
        l.posttypeid = element.get('PostTypeId')
        l.parentid = element.get('ParentId')
        l.acceptedanswerid = element.get('AcceptedAnswerId')
        l.creationdate = element.get('CreationDate')
        l.score = element.get('Score')
        l.viewcount = element.get('ViewCount')
        l.body = element.get('Body')
        l.owneruserid = element.get('OwnerUserId')
        l.lasteditoruserid = element.get('LastEditorUserId')
        l.lasteditordisplayname = element.get('LastEditorDisplayName')
        l.lasteditdate = element.get('LastEditDate')
        l.lastactivitydate = element.get('LastActivityDate')
        l.communityowneddate = element.get('CommunityOwnedDate')
        l.closeddate = element.get('ClosedDate')
        l.title = element.get('Title')
        l.tags = element.get('Tags')
        l.answercount = element.get('AnswerCount')
        l.commentcount = element.get('CommentCount')
        l.favoritecount = element.get('FavoriteCount')
        l.sitename = os.path.dirname(os.path.realpath(__file__))

	#Calling checking function
	l_validated=validate(l)

        return l_validated

class Users(Base):
    __tablename__ = 'users'

    UID=Column(INTEGER, primary_key=True,autoincrement=True )
    id = Column(INTEGER,default="-9999")
    reputation = Column(INTEGER,default="-9999")
    creationdate =Column(TIMESTAMP)
    displayname = Column(VARCHAR)
    emailhash = Column(VARCHAR)
    lastaccessdate =Column(TIMESTAMP)
    websiteurl = Column(VARCHAR)
    location = Column(VARCHAR)
    age = Column(SMALLINT,default="-9999")
    aboutme = Column(VARCHAR)
    views = Column(INTEGER,default="-9999")
    upvotes = Column(INTEGER,default="-9999")
    downvotes = Column(INTEGER,default="-9999")
    #filepath=Column(VARCHAR)
    #sitename=Column(VARCHAR)

    @classmethod
    def from_xmlgroup(cls, element):
        l = cls() #used as the first arguments to class arguments
        l.id = element.get('Id')
        l.reputation = element.get('Reputation')
        l.creationdate = element.get('CreationDate')
        l.displayname = element.get('DisplayName')
        l.emailhash = element.get('EmailHash')
        l.lastaccessdate = element.get('LastAccessDate')
        l.websiteurl = element.get('WebsiteUrl')
        l.location = element.get('Location')
        l.age = element.get('Age')
        l.aboutme = element.get('AboutMe')
        l.views = element.get('Views')
        l.upvotes = element.get('UpVotes')
        l.downvotes = element.get('DownVotes')
        l.sitename = os.path.dirname(os.path.realpath(__file__))
        return l

class Votes(Base):
    __tablename__ = 'votes'

    UID=Column(INTEGER, primary_key=True,autoincrement=True )
    id = Column(INTEGER,default="-9999")
    postid = Column(INTEGER,default="-9999")
    votetypeid = Column(SMALLINT,default="9999")
    creationdate =Column(TIMESTAMP)
    userid = Column(INTEGER,default="-9999")
    bountyamount = Column(INTEGER,default="-9999")
    #filepath=Column(VARCHAR)
    #sitename=Column(VARCHAR)

    @classmethod
    def from_xmlgroup(cls, element):
        l = cls() #used as the first arguments to class arguments
        l.id = element.get('Id')
        l.postid = element.get('PostId')
        l.votetypeid = element.get('VoteTypeId')
        l.creationdate = element.get('CreationDate')
        l.userid = element.get('UserId')
        l.bountyamount = element.get('BountyAmount')
        l.sitename = os.path.dirname(os.path.realpath(__file__))
        return l

class Comments(Base):
    __tablename__ = 'comments'

    UID=Column(INTEGER, primary_key=True,autoincrement=True )
    id = Column(INTEGER,default ="-9999")
    postid = Column(INTEGER,default="-9999")
    score = Column(INTEGER,default="-9999")
    text = Column(VARCHAR)
    creationdate =Column(TIMESTAMP)
    userid = Column(INTEGER,default="-9999")
    #filepath = Column(VARCHAR)
    #sitename=Column(VARCHAR)

    @classmethod
    def from_xmlgroup(cls, element):
        l = cls() #used as the first arguments to class arguments
        l.id = element.get('Id')
        l.postid = element.get('PostId')
        l.score = element.get('Score')
        l.text = element.get('Text')
        l.creationdate = element.get('CreationDate')
        l.userid = element.get('UserId')
        l.sitename = os.path.dirname(os.path.realpath(__file__))
        return l

class PostHistory(Base):
    __tablename__ = 'postHistory'

    UID=Column(INTEGER, primary_key=True,autoincrement=True )
    id = Column(INTEGER,default="-9999")
    posthistorytypeid = Column(INTEGER,default="-9999")
    postid = Column(INTEGER,default="-9999")
    revisionguid = Column(UUID,default="-9999")
    creationdate =Column(TIMESTAMP)
    userid = Column(INTEGER,default="-9999")
    userdisplayname = Column(VARCHAR)
    comment = Column(VARCHAR)
    text = Column(VARCHAR)
    closereasonid = Column(INTEGER,default="-9999")
    #filepath=Column(VARCHAR)
    #sitename=Column(VARCHAR)

    @classmethod
    def from_xmlgroup(cls, element):
        l = cls() #used as the first arguments to class arguments
        l.id = element.get('Id')
        l.posthistorytypeid = element.get('PostHistoryTypeId')
        l.postid = element.get('PostId')
        l.revisionguid = element.get('RevisionGUID')
        l.creationdate = element.get('CreationDate')
        l.userid = element.get('UserId')
	l.userdisplayname = element.get('UserDisplayName')
	l.comment = element.get('Comment')
	l.text = element.get('Text')
	l.closereasonid = element.get('CloseReasonId')
	l.sitename = os.path.dirname(os.path.realpath(__file__))
        return l

class PostLinks(Base):
    __tablename__ = 'postLinks'

    UID=Column(INTEGER, primary_key=True,autoincrement=True )
    id = Column(INTEGER,default="-9999")
    creationdate =Column(TIMESTAMP)
    postid = Column(INTEGER,default="-9999")
    relatedpostid = Column(INTEGER,default="-9999")
    postlinktypeid = Column(INTEGER,default="-9999")
    #filepath=Column(VARCHAR)
    #sitename=Column(VARCHAR)

    @classmethod
    def from_xmlgroup(cls, element):
        l = cls() #used as the first arguments to class arguments
        l.id = element.get('Id')
        l.creationdate = element.get('CreationDate')
	l.postid = element.get('PostId')
	l.relatedpostid = element.get('RelatedPostId')
	l.postlinktpeid = element.get('PostLinkTypeId')
	l.sitename = os.path.dirname(os.path.realpath(__file__))
        return l


class Tags(Base):
    __tablename__ = 'tags'

    UID=Column(INTEGER, primary_key=True,autoincrement=True )
    id=Column(INTEGER,default = "-9999")
    tag_Name=Column(VARCHAR)
    tag_count=Column(INTEGER)
    ExcerptPostId=Column(INTEGER)
    WikiPostId=Column(INTEGER)
    #filepath=Column(VARCHAR)
    #sitename=Column(VARCHAR)

    @classmethod
    def from_xmlgroup(cls,element):
        l= cls()
        l.id = element.get('Id')
        l.tag_Name = element.get('TagName')
        l.tag_count = element.get('Count')
        l.ExcerptPostId = element.get('ExcerptPostId')
        l.WikiPostId = element.get('WikiPostId')
        l.sitename = os.path.dirname(os.path.realpath(__file__))
        return l


def createTables():
    Base.metadata.create_all() # creates the table
    session = create_session(bind=engine, autocommit=True)


def walkdir():
    print("walking directories")

    #f = open('output.txt','w')
    filenamemylist=[]
    for dirname, dirnames, filenames in os.walk(DATADIR):
		for filename in filenames:
			name = os.path.join(dirname,filename)
			#print ("name",name)
			#f.write(name + "\n")
			filenamemylist.append(name)
    #f.close()
    #print (filenamemylist)
    return filenamemylist


	
def getfilenames(mylist, termOfInterest):
    print("Getting filename")
    logger.info(mylist)
	
    ##read completed file list
    #with open('../log/completedFiles.txt') as f:
    #    completed_list=f.read().splitlines()
    #    #print(completed_list)
    #    f.close()
    ##remove all completed files
    #for i in mylist[:]:
    #    if i in completed_list:
    #        mylist.remove(i)
    #print(mylist)

    for item in mylist:
        logger.info("Entering"+str(item))
        if (('badges' in item) or ('Badges' in item))and (termOfInterest=='b'):
            inserttodatabases(item,Badges)
            logger2.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") +" "+ str(item))
            print("done "+str(item)+ datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        elif (('users' in item) or ('Users' in item)) and (termOfInterest=='u'):
            inserttodatabases(item,Users)
            logger2.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") +" "+ str(item))
            print("done "+str(item)+ datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        elif (('posts' in item)or ('Posts' in item)) and (termOfInterest=='p'):
            inserttodatabases(item,Posts)
            logger2.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") +" "+ str(item))
            print("done "+str(item)+ datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        elif (('votes' in item) or ('Votes' in item)) and (termOfInterest=='v'):
            inserttodatabases(item,Votes)
            logger2.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") +" "+ str(item))
            print("done "+str(item)+ datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        elif (('comments' in item) or ('Comments' in item)) and (termOfInterest=='c'):
            inserttodatabases(item,Comments)
            logger2.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") +" "+ str(item))
            print("done "+str(item)+ datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        elif (('posthistory' in item) or ('PostHistory' in item)) and (termOfInterest=='h'):
            inserttodatabases(item,PostHistory)
            logger2.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") +" "+ str(item))
            print("done "+str(item)+ datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        elif (('Tags' in item) or ('Tags' in item)) and (termOfInterest=='t'):
            inserttodatabases(item,Tags)
            logger2.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") +" "+ str(item))
            print("done "+str(item)+ datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    	elif (('postlinks' in item) or ('PostLinks' in item))and (termOfInterest=='l'):
            inserttodatabases(item,PostLinks)
            logger2.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") +" "+ str(item))
            print("done "+str(item)+ datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        else:
            pass

#def inserttodatabases(filepaths, clsname):
#    pass

def inserttodatabases(filepaths,clsname):
    my_object = []
    Base.metadata.create_all() # creates the table
    session = create_session(bind=engine, autocommit=True)
    fhandle=open(filepaths)
    dirtyxml=fhandle.read()
    cleanxml = illegal_xml_re.sub('', dirtyxml)
    root = etree.fromstring(cleanxml, parser=etree.XMLParser(recover=True))
    for elem in root.iter('row'):
        try:
            l = clsname.from_xmlgroup(elem)
        except Exception,e:
            #print("Exception--"+str(elem.text))
            logger.exception(e)
            continue
        l.filepath = filepaths
        l.sitename = filepaths.split('/')[-2]
        my_object.append(l)
        elem.clear()
        del elem
	
        if len(my_object) == 10000:
            try:
                pass
                session.add_all(my_object)
	        #print ("done  insertion @"+ datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            except Exception,e:
                logger.exception(e)
                continue
            session.flush()
            my_object = []

    if my_object:
        session.add_all(my_object)
        #print('added')
        session.flush()


def getArgs():
    if len(sys.argv) <2:
        sys.stderr.write("use only one arg: b-badges, u-users,p-posts, v-votes, c-comments, h-posthistory, l-postlinks t-tags \n ")
        raise SystemExit(1)
    else:
        termName=sys.argv[3]
        #print(termName)
        return(termName)


if __name__ == '__main__':
    #1. did you check the datatypes of the create tables - check carefully each of the datatypes
    print(illegal_ranges)
    termOfInterest = getArgs()
    mylist = []
    createTables()
    mylist = walkdir()
    #print ('mu list',mylist[0])
    getfilenames(mylist, termOfInterest)
    # run it on the data for all the 6 tables and see if it works. Then we can
    # start on production run.in multiple machines

    # get command line agruments (import sys and sys.args.....) as badges,
    # posts, PostHistory, users, ....... as a string argument and assign it to
    # a variable. Then write a ifelse statement for running the appoprite
    # tables

    # call the function inserttodatabase
    # a variable. Then write a ifelse statement for running the appoprite
    # tables

    # call the function inserttodatabase
