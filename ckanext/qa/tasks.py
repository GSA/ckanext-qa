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
            # ct = get_content_type(data['url'])
            # ct = resource.get_content_type()

            # ignore charset if exists (just take everything before the ';')
            # if ct and ';' in ct:
            # ct = ct.split(';')[0]

            # also get format from resource and by guessing from file extension
            # resource_format = data.get('format', '').lower()
            # file_type = mimetypes.guess_type(data['url'])[0]

            # score = MIME_TYPE_SCORE.get(ct, -1)
            # file type takes priority for scoring
            # if file_type:
            # score = MIME_TYPE_SCORE.get(file_type, -1)
            # elif ct:
            # score = MIME_TYPE_SCORE.get(ct, -1)
            # elif resource_format:
            # score = MIME_TYPE_SCORE.get(resource_format, -1)

            ct = resource.get_content_type()
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

        # except LinkCheckerError, e:
        # score_reason = str(e)
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


# def get_content_type(url):
# d = urllib2.urlopen(url, timeout=120)
# return d.info()['Content-Type']
#
#
# def get_error_code(url):
# req = urllib2.Request(url)
# try:
# urllib2.urlopen(req, timeout=120)
# return 0
# except urllib2.HTTPError, e:
# return e.code
# except URLError, e:
# return 408

URL_REGEX = re.compile(
    r'^(?:http|ftp)s?://'   # http:// or https:// or ftp:// or ftps://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)


class RemoteResource(object):
    # list of errors we want to avoid wasting time on.
    ERRNO_BLOCK = {
        101: 'Network is unreachable',
    }
    BLOCK_THRESHOLD = 15
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
                self.status_code = 200
                return
            except urllib2.URLError, e:
                self.status_code = 500
                return None
            except urllib2.HTTPError, e:
                return e.code
            except Exception, e:
                return 408

        blacklist_errno = self.check_url_blacklist(self.url)
        if blacklist_errno:
            self.status_code = 500
            self.reason = self.ERRNO_BLOCK.get(blacklist_errno)
            log.error('get_content_type blacklisted ( %s ): %s: %s ' % (
                    self.url, self.status_code, self.reason))
            return None

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
            self.delete_url_blacklist(self.url)
            return self.content_type

        except Exception as ex:
            self.status_code = 500
            self.reason = ex.__class__.__name__
            log.error('get_content_type exception ( %s ): %s ' % (self.url, ex))
            errno = ex.args[0].reason.errno
            if errno in self.ERRNO_BLOCK.keys():
                self.add_url_blacklist(self.url, errno)

            return None

    def valid_url(self):
        return URL_REGEX.match(self.url)

    def get_error_code(self):
        if self.status_code < 400:
            return 0
        return self.status_code

    def add_url_blacklist(self, url, errno):
        url_paths = self.get_url_paths(url)
        url_paths.reverse() # so we deal with closest parent first
        sql_SELECT = '''
                SELECT path
                FROM resource_domain_blacklist
                WHERE path = :path;
        '''
        sql_UPDATE = '''
                UPDATE resource_domain_blacklist
                SET count = count +1
                WHERE path = :path;
        '''
        sql_INSERT = '''
                INSERT INTO resource_domain_blacklist(path, count, errno)
                VALUES (:path, 1, :errno);
        '''
        for path in url_paths:
            q = model.Session.execute(sql_SELECT, {'path': path})
            rowcount = q.rowcount
            if rowcount:
                model.Session.execute(sql_UPDATE, {'path': path})
                model.Session.commit()
                break # only update closest existing parent
            else:
                model.Session.execute(sql_INSERT, {
                        'path': path,
                        'errno':errno, # todo deal with multiple errno
                })
                model.Session.commit()

    def delete_url_blacklist(self, url):
        url_paths = self.get_url_paths(url)
        sql_DELETE = '''
                DELETE FROM resource_domain_blacklist
                WHERE path = :path;
        '''
        for path in url_paths:
            q = model.Session.execute(sql_DELETE, {'path': path})
            model.Session.commit()

    @staticmethod
    def clear_url_blacklist():
        sql_CLEAR = '''
                DELETE FROM resource_domain_blacklist;
        '''
        q = model.Session.execute(sql_CLEAR)
        model.Session.commit()

    def check_url_blacklist(self, url):
        errno = None
        url_paths = self.get_url_paths(url)
        sql_CHECK = '''
                SELECT errno
                FROM resource_domain_blacklist
                WHERE path = :path
                AND count >= :count
                LIMIT 1;
        '''
        for path in url_paths:
            q = model.Session.execute(sql_CHECK, {
                    'path': path,
                    'count': self.BLOCK_THRESHOLD,
            })
            if q.rowcount:
                errno = q.fetchone()[0]
                break

        return errno

    def get_url_paths(self, url):
        from urlparse import urlparse
        url_paths = []

        o = urlparse(url)
        if not o.scheme and not o.netloc:
            return url_paths

        current_path = o.scheme + '://' + o.netloc.lower()
        url_paths.append(current_path)
        paths = o.path.split('/')
        for path in paths:
            if path:
                current_path = current_path + '/' + path
                url_paths.append(current_path)

        return url_paths

