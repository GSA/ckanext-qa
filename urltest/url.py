import requests
import re
import urllib2

URL_REGEX = re.compile(
    r'^(?:http|ftp)s?://'   # http:// or https:// or ftp:// or ftps://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

class RemoteResource(object):
    def __init__(self, url):
        self.url = url.strip()
        self.status_code = 0
        self.reason = ''
        self.method = None
        self.content_type = None
        self.headers = {'User-Agent': 'Data.Gov Broken Link Checker'}

    def get_content_type(self):
        if not self.valid_url():
            self.status_code = 400
            self.reason = 'Invalid URL'
            self.content_type = ''
            return self.content_type

        if 'ftp://' in self.url:
            req = urllib2.Request(self.url)

            try:
                answer = urllib2.urlopen(req, timeout=20)
                self.content_type = answer.headers.get('content-type')
                self.status_code = 200
                return 0
            except urllib2.HTTPError, e:
                return e.code
            except Exception, e:
                return 408

        try:
            # http://docs.python-requests.org/en/latest/api/
            method = 'HEAD'
            r = requests.head(self.url, verify=False, timeout=20.0, allow_redirects=True, headers=self.headers)

            if r.status_code > 399 or r.headers.get('content-type') is None:
                method = 'GET'
                r = requests.get(self.url, verify=False, timeout=20.0, allow_redirects=True, stream=True,
                                 headers=self.headers)
                head = r.raw.read(50)
                print 'head: %s' % head
                print 'status code: %s' % r.status_code
                print 'content type: %s' % r.headers.get('content-type')
                # r.raw.read(50)
                if r.status_code > 399 or r.headers.get('content-type') is None:
                    self.status_code = r.status_code
                    self.reason = r.reason
                    self.method = method
                    self.content_type = None
                    return self.content_type

            content_type = r.headers.get('content-type')
            self.content_type = content_type.split(';', 1)[0]
            self.status_code = r.status_code
            self.reason = r.reason
            self.method = method
            return self.content_type

        except Exception as ex:
            self.status_code = 500
            self.reason = ex.__class__.__name__

            print 'get_content_type exception: %s \n%s' % (ex, self.url)

            return None

    def valid_url(self):
        return URL_REGEX.match(self.url)

    def get_error_code(self):
        if self.status_code < 400:
            return 0
        return self.status_code


resources = set()
skip = set()

f = open('rresources.csv', 'r')
# f = open('resources_test.csv', 'r')

for line in f:
    domain = line[:30]
    if domain not in skip:
        resources.add(RemoteResource(line))
        skip.add(domain)


# resources.add(RemoteResource(line))
# resources.add(RemoteResource('http://catalog.data.gov/api/rest/dataset/consumer-complaint-database'))
# resources.add(RemoteResource('http://images.all-free-download.com/images/graphiclarge/mortal_kombat_139192.jpg'))
# resources.add(RemoteResource('https//yatin.com/blabla.pdf'))
# resources.add(RemoteResource('http://data.consumerfinance.gov/api/views.json'))
# resources.add(RemoteResource('http://data.consumerfinance.gov/api/views/x94z-ydhh.json'))
# resources.add(RemoteResource('https://data.consumerfinance.gov/api/views/x94z-ydhh/rows.csv?accessType=DOWNLOAD'))
# resources.add(RemoteResource('http://www.census.gov/foreign-trade/statistics/historical/'))
# resources.add(RemoteResource('http://factfinder2.census.gov/faces/nav/jsf/pages/searchresults.xhtml?refresh=t'))
# resources.add(RemoteResource('https://data.illinois.gov/api/views/rjxh-tv66/rows.json?accessType=DOWNLOAD'))
# resources.add(RemoteResource('https://data.wa.gov/api/views/vsr8-3iup/rows.json?accessType=DOWNLOAD'))
# resources.add(RemoteResource('http://ecos.fws.gov/ServCatFiles/reference/holding/40288?accessType=DOWNLOAD'))
# resource = RemoteResource('http://asdfasdf.com/asdf')
# no score for resources that don't have an open license

for resource in resources:
    try:
        # ct = get_content_type(data['url'])

        ct = resource.get_content_type()

        # if resource.status_code < 399:
        #     continue

        print ct
        print "URL: \t\t\t%s" % resource.url
        print "METHOD: \t\t%s" % resource.method
        print "STATUS CODE: \t%s" % resource.status_code
        print "REASON: \t\t%s" % resource.reason
        print "CONTENT TYPE: \t\t%s" % resource.content_type
        print "========================================"

        if not ct:
            # TODO: use the todo extension to flag this issue
            pass

    except Exception, e:
        print 'Unexpected error while calculating openness score %s: %s' % (e.__class__.__name__, unicode(e))
        score_reason = "Unknown error: %s" % str(e)


        # error_code = get_error_code(data['url'])
        # error_code = resource.get_error_code()