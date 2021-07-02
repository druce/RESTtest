import sys
import time
import io
import yaml
import requests
from xml.etree import ElementTree

# just run once and return error code, have scheduler or visualcron run it
# logfile
# print errors to stderr

statefile = 'changefeed.yaml'
login = '*************'
pw = '********************'
get_endpoint = 'https://wolferesearch-test.bluematrix.com/sellside/ChangeFeed.action?firmId=96533&mode=system'
post_endpoint = 'http://httpbin.org/post'
sleep_delay = 60

testxml = """<?xml version="1.0" encoding="UTF-8"?>
<DataFeed>
    <timeframe>2021-03-21T12:03:06.00</timeframe>
    <now>2021-03-22T12:03:06.00</now>
    <ChangeEvent  EventId="2792" FeedName="Content" ImportType="I" Parameter="14" ChangeType="add" Date="2021-03-22T09:10:07.00" CurrentState="Published" CompletedState="" XmlUrl="https://wolferesearch-test.bluematrix.com/docs/xml/ff8ea0f5-7d53-47d2-a345-bf75a58057ef.xml" Url="https://wolferesearch-test.bluematrix.com/sellside/ChangeFeed.action?firmId=96533&amp;mode=Content&amp;param=14" />
    <ChangeEvent  EventId="2793" FeedName="Content" ImportType="I" Parameter="14" ChangeType="statusChanged" Date="2021-03-22T09:13:41.00" CurrentState="Published" CompletedState="Author" XmlUrl="https://wolferesearch-test.bluematrix.com/docs/xml/ff8ea0f5-7d53-47d2-a345-bf75a58057ef.xml" Url="https://wolferesearch-test.bluematrix.com/sellside/ChangeFeed.action?firmId=96533&amp;mode=Content&amp;param=14" />
    <ChangeEvent  EventId="2794" FeedName="Content" ImportType="I" Parameter="14" ChangeType="aqadd" Date="2021-03-22T09:13:41.00" CurrentState="Published" CompletedState="" XmlUrl="https://wolferesearch-test.bluematrix.com/docs/xml/ff8ea0f5-7d53-47d2-a345-bf75a58057ef.xml" Url="https://wolferesearch-test.bluematrix.com/sellside/ChangeFeed.action?firmId=96533&amp;mode=Content&amp;param=14" />
    <ChangeEvent  EventId="2795" FeedName="Content" ImportType="I" Parameter="14" ChangeType="statusChanged" Date="2021-03-22T09:13:52.00" CurrentState="Published" CompletedState="Editorial" XmlUrl="https://wolferesearch-test.bluematrix.com/docs/xml/ff8ea0f5-7d53-47d2-a345-bf75a58057ef.xml" Url="https://wolferesearch-test.bluematrix.com/sellside/ChangeFeed.action?firmId=96533&amp;mode=Content&amp;param=14" />
    <ChangeEvent  EventId="2796" FeedName="Content" ImportType="I" Parameter="14" ChangeType="statusChanged" Date="2021-03-22T09:14:08.00" CurrentState="Published" CompletedState="SA" XmlUrl="https://wolferesearch-test.bluematrix.com/docs/xml/ff8ea0f5-7d53-47d2-a345-bf75a58057ef.xml" Url="https://wolferesearch-test.bluematrix.com/sellside/ChangeFeed.action?firmId=96533&amp;mode=Content&amp;param=14" />
    <ChangeEvent  EventId="2797" FeedName="Content" ImportType="I" Parameter="14" ChangeType="statusChanged" Date="2021-03-22T09:14:13.00" CurrentState="Published" CompletedState="Publish" XmlUrl="https://wolferesearch-test.bluematrix.com/docs/xml/ff8ea0f5-7d53-47d2-a345-bf75a58057ef.xml" Url="https://wolferesearch-test.bluematrix.com/sellside/ChangeFeed.action?firmId=96533&amp;mode=Content&amp;param=14" />
    <ChangeEvent  EventId="2798" FeedName="Content" ImportType="I" Parameter="14" ChangeType="pub" Date="2021-03-22T09:14:26.00" CurrentState="Published" CompletedState="Publish" XmlUrl="https://wolferesearch-test.bluematrix.com/docs/xml/ff8ea0f5-7d53-47d2-a345-bf75a58057ef.xml" Url="https://wolferesearch-test.bluematrix.com/sellside/ChangeFeed.action?firmId=96533&amp;mode=Content&amp;param=14" />
    <ChangeEvent  EventId="2799" FeedName="Content" ImportType="I" Parameter="15" ChangeType="add" Date="2021-03-22T11:21:22.00" CurrentState="Author" CompletedState="" XmlUrl="https://wolferesearch-test.bluematrix.com/sellside/DocViewer?firmId=96533&amp;&amp;encrypt=3a361ec1-46e1-4e1f-9db2-de96ba4d21ea&amp;mime=XML" Url="https://wolferesearch-test.bluematrix.com/sellside/ChangeFeed.action?firmId=96533&amp;mode=Content&amp;param=15" />
    <ChangeEvent  EventId="2800" FeedName="Elements" ImportType="I" Parameter="231" ElementType="Clusters" ChangeType="mod" Date="2021-03-22T11:21:55.00" Url="https://wolferesearch-test.bluematrix.com/sellside/ChangeFeed.action?firmId=96533&amp;mode=Elements&amp;param=231" />
</DataFeed>
"""


def read_state(statefile):
    """read state dict (with last EventID) from YAML file"""
    try:
        with open(statefile, 'r', encoding='utf8') as stream:
            data = yaml.safe_load(stream)
            print(data)
    except yaml.YAMLError as exc:
        print(exc)
        return None

    return data


def write_state(statefile, state):
    """write state dict (with last EventID) to YAML file"""

    try:
        with io.open(statefile, 'w', encoding='utf8') as outfile:
            yaml.dump(state, outfile, default_flow_style=False,
                      allow_unicode=True)
    except Exception as exc:
        print(exc)


def get_latest(lastEventId=None):
    """get latest and return content"""
    if lastEventId:
        # get all
        endpoint = 'https://wolferesearch-test.bluematrix.com/sellside/ChangeFeed.action?firmId=96533&mode=system'
    else:
        # get since last event
        endpoint = 'https://wolferesearch-test.bluematrix.com/sellside/ChangeFeed.action?firmId=96533&mode=system'
    try:
        request = requests.get(endpoint, auth=(login, pw))
        # process xml
        if request.ok:
            xmlstr = request.content
            return xmlstr
        else:
            return ""
    except Exception as exc:
        print(exc)
        return ""


def post_latest(json):
    try:
        r = requests.post(post_endpoint, json)
        if r.ok:  # status_code == 200:
            print(r.status_code, r.json())
            return 0
        else:
            print(r.status_code, "POST failed")
            return -1
    except Exception as exc:
        print(exc)
        return -1


# get state
state = {}
if statefile:
    state = read_state(statefile)

while True:
    # get latest events as XML
    xmlstr = get_latest(state.get('lastEventId'))
    if xmlstr:
        try:
            # parse XML
            root = ElementTree.fromstring(xmlstr)
        except Exception as exc:
            print(exc)
    else:
        print("failed to get XML")

    # loop through events that represent new reports
    count = 0
    for child in root:
        if child.tag == "ChangeEvent":
            currentState = child.attrib.get('CurrentState')
            if currentState == 'Published':
                # post report
                json = {"EventId": child.attrib['EventId'],
                        "XmlUrl": child.attrib['XmlUrl']}
                if post_latest(json):
                    # save latest event successfully processed
                    state['lastEventId'] = max(child.attrib['EventId'], state['lastEventId'])
                    write_state(statefile, state)
                    count += 1
                else:
                    print("failed to post JSON")

    if count:
        print("Posted new items: %d" % count)
    sys.stdout.write(".")
    time.sleep(sleep_delay)
