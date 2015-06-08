"""
Score datasets on Sir Tim Berners-Lee\'s five stars of openness
based on mime-type.
"""

import json
import urlparse
import urllib2
import logging
import datetime

import requests
import re
import ckan.lib.celery_app as celery_app
from ckan import model


log = logging.getLogger('ckanext.qa')


class QAError(Exception):
    pass


class CkanError(Exception):
    pass


OPENNESS_SCORE_REASON = {
    -1: 'unrecognised content type',
    0: 'not obtainable',
    1: 'obtainable via web page',
    2: 'machine readable format',
    3: 'open and standardized format',
    4: 'ontologically represented',
    5: 'fully Linked Open Data as appropriate',
}

MIME_TYPE_SCORE = {
    'text/plain': 1,
    'text/html': 1,
    'text': 1,
    'txt': 1,
    'application/vnd.ms-excel': 2,
    'application/vnd.ms-excel.sheet.binary.macroenabled.12': 2,
    'application/vnd.ms-excel.sheet.macroenabled.12': 2,
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 2,
    'xls': 2,
    'text/csv': 3,
    'application/json': 3,
    'application/xml': 3,
    'text/xml': 3,
    'csv': 3,
    'xml': 3,
    'json': 3,
    'application/rdf+xml': 4,
    'rdf': 4
}


def _update_task_status(context, data):
    """
    Use CKAN API to update the task status. The data parameter
    should be a dict representing one row in the task_status table.

    Returns the content of the response.
    """
    api_url = urlparse.urljoin(context['site_url'], 'api/action')

    url = api_url + '/task_status_update'
    params = '%s=1' % json.dumps({'data': data})
    headers = {"Accept": "application/json",
               "Conthent-Type": "application/json",
               'Authorization': context['apikey']
               }

    req = urllib2.Request(url, params, headers)
    try:
        response = urllib2.urlopen(req, timeout=120)
    except urllib2.HTTPError as e:
        # raise CkanError('The server couldn\'t fulfill the request. Error code: %s'
        # % (e.code))
        log.error('The server couldn\'t fulfill the request. Error code: %s'
                  % (e.code))
    except urllib2.URLError as e:
        # raise CkanError('We failed to reach a server. Reason: %s'
        # % (e.reason))
        log.error('We failed to reach a server. Reason: %s'
                  % (e.reason))
    except:
        pass
    else:
        f = response.read()
        content = json.loads(f)

        if content.getcode() == 200:
            return json.dumps(content.get('result').get('results'))
        else:
            # raise CkanError('ckan failed to update task_status, status_code (%s), error %s'
            # % (res.status_code, res.content))
            log.error('ckan failed to update task_status, status_code (%s), error %s'
                      % (res.status_code, res.content))


def _task_status_data(id, result):
    return [
        {
            'entity_id': id,
            'entity_type': u'resource',
            'task_type': 'qa',
            'key': u'openness_score',
            'value': result['openness_score'],
            'last_updated': datetime.datetime.now().isoformat()
        },
        {
            'entity_id': id,
            'entity_type': u'resource',
            'task_type': 'qa',
            'key': u'openness_score_reason',
            'value': result['openness_score_reason'],
            'last_updated': datetime.datetime.now().isoformat()
        },
        {
            'entity_id': id,
            'entity_type': u'resource',
            'task_type': 'qa',
            'key': u'openness_score_failure_count',
            'value': result['openness_score_failure_count'],
            'last_updated': datetime.datetime.now().isoformat()
        },
        {
            'entity_id': id,
            'entity_type': u'resource',
            'task_type': 'qa',
            'key': u'error_code',
            'value': result['error_code'],
            'last_updated': datetime.datetime.now().isoformat()
        },
    ]


@celery_app.celery.task(name="qa.update")
def update(context, data):
    """
    Score resources on Sir Tim Berners-Lee\'s five stars of openness
    based on mime-type.
    
    Returns a JSON dict with keys:

        'openness_score': score (int)
        'openness_score_reason': the reason for the score (string)
        'openness_score_failure_count': the number of consecutive times that
                                        this resource has returned a score of 0
    """
    # log = update.get_logger()
    try:
        data = json.loads(data)
        context = json.loads(context)

        result = resource_score(context, data)

        log.info('Openness score for dataset %s (res#%s): %r (%s)',
                 data['package'], data['position'],
                 result['openness_score'], result['openness_score_reason'])

        task_status_data = _task_status_data(data['id'], result)
        api_url = urlparse.urljoin(context['site_url'], 'api/action')

        url = api_url + '/task_status_update_many'
        params = '%s=1' % json.dumps({'data': task_status_data})
        headers = {"Accept": "application/json",
                   "Conthent-Type": "application/json",
                   'Authorization': context['apikey']
                   }

        req = urllib2.Request(url, params, headers)
        response = urllib2.urlopen(req, timeout=120)
        f = response.read()
        content = json.loads(f)

        if not content.get('success'):
            err = 'ckan failed to update task_status, error %s' \
                  % content['error']
            log.error(err)
        elif response.getcode() != 200:
            err = 'ckan failed to update task_status, status_code (%s), error %s' \
                  % (response.getcode(), content.get('result').get('results'))
            log.error(err)

        return json.dumps(content.get('result').get('results'))
    except Exception, e:

        log.error('Exception occurred during QA update: %s: %s', e.__class__.__name__, unicode(e))
        _update_task_status(context, {
            'entity_id': data['id'],
            'entity_type': u'resource',
            'task_type': 'qa',
            'key': u'celery_task_id',
            'value': unicode(update.request.id),
            'error': '%s: %s' % (e.__class__.__name__, unicode(e)),
            'last_updated': datetime.datetime.now().isoformat()
        })


def resource_score(context, data):
    """
    Score resources on Sir Tim Berners-Lee\'s five stars of openness
    based on mime-type.

    returns a dict with keys:

        'openness_score': score (int)
        'openness_score_reason': the reason for the score (string)
        'openness_score_failure_count': the number of consecutive times that
                                        this resource has returned a score of 0
    """
    # log = update.get_logger()

    score = 0
    # score_reason = ''
    score_failure_count = 0
    # error_code = 0

    # get openness score failure count for task status table if exists
    api_url = urlparse.urljoin(context['site_url'], 'api/action')

    args = {'entity_id': data['id'], 'task_type': 'qa',
            'key': 'openness_score_failure_count'}

    response = requests.get(api_url + '/task_status_show', params=args).text

    if json.loads(response)['success']:
        score_failure_count = int(json.loads(response)['result'].get('value', '0'))

    resource = RemoteResource(data['url'])

    # no score for resources that don't have an open license
    if not data.get('is_open') and 2 < 1:
        score_reason = 'License not open'
    else:
        try:
        
            domain_url = resource.url.split("/")
            if len(domain_url) > 3:
                domain_url.pop()
        
            domain_url =  "/".join(domain_url)
            
            sql = '''select status, count from resource_domain_info where domain = :domain_url;'''
            q = model.Session.execute(sql, {'domain_url' : domain_url})
            
            num = q.rowcount
 
            if num > 0:        
                for row in q:
                    if row['count'] == 10 and row['status'] == 'blacklist':
                        resource.status_code = 500
                        ct = None
                        break
                    
                    ct = resource.get_content_type()    
                    if resource.status_code >=500 and resource.status_code < 600 and row[0] == 'undecided':
                        count = row['count'] + 1
                        if count == 10:
                            sql = '''update resource_domain_info set count = :count, status='blacklist' where domain = :domain;'''
                            model.Session.execute(sql, {'count':count, 'domain': domain_url})
                        else:
                            sql = '''update resource_domain_info set count = :count where domain = :domain;'''
                            model.Session.execute(sql, {'count':count, 'domain': domain_url})
                    else:
                        sql = '''update resource_domain_info set status='whitelist' where domain = :domain; '''
                        model.Session.execute(sql, {'domain': domain_url})                   
                    model.Session.commit()
            else:
                ct = resource.get_content_type()
                if resource.status_code >=500 and resource.status_code < 600:
                    status = 'undecided'
                    num = num + 1
                else:
                    status = 'whitelist'
                
                sql = '''INSERT INTO resource_domain_info (domain, status, count) VALUES(:domain_url, :status, :num);'''
                model.Session.execute(sql, {'domain_url':domain_url, 'status':status, 'num':num})
                model.Session.commit() 
        
            score = MIME_TYPE_SCORE.get(ct, -1)

            if not data.get('is_open'):
                score = 0

            score_reason = OPENNESS_SCORE_REASON[score]

            # negative scores are only useful for getting the reason message,
            # set it back to 0 if it's still <0 at this point
            if score < 0:
                score = 0

            # check for mismatches between content-type, file_type and format
            # ideally they should all agree
            if not ct:
                # TODO: use the todo extension to flag this issue
                pass

        except Exception, e:
            log.error('Unexpected error while calculating openness score %s: %s', e.__class__.__name__, unicode(e))
            score_reason = "Unknown error: %s" % str(e)

    if score == 0:
        score_failure_count += 1
    else:
        score_failure_count = 0

    # error_code = get_error_code(data['url'])
    error_code = resource.get_error_code()

    return {
        'openness_score': score,
        'openness_score_reason': score_reason,
        'openness_score_failure_count': score_failure_count,
        'error_code': error_code,
    }

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
                return self.content_type
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
                r.raw.read(50)
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
            log.error('get_content_type exception ( %s ): %s ' % (self.url, ex))

            return None

    def valid_url(self):
        return URL_REGEX.match(self.url)

    def get_error_code(self):
        if self.status_code < 400:
            return 0
        return self.status_code