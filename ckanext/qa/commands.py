import datetime
import json
import requests
import urlparse
import logging
from pylons import config
from urllib2 import Request, urlopen, URLError, HTTPError
import math
import os.path

import ckan.plugins as p
from ckan import model


REQUESTS_HEADER = {'content-type': 'application/json'}

class CkanApiError(Exception):
    pass

# @TODO: use ORM + sqlalchemy to work with the db
class QACommand(p.toolkit.CkanCommand):
    """
    QA analysis of CKAN resources

    Usage::

        paster qa [options] update [dataset name/id]
           - QA analysis on all resources in a given dataset, or on all
           datasets if no dataset given

        paster qa update_sel
           - QA analysis on all datasets whose last modified timestamp 
           is >= than the timestamp from 
           `log_last_modified = '/var/log/qa-metadata-modified.log'` 

        paster qa clean
            - Remove all package score information

    The commands should be run from the ckanext-qa directory and expect
    a development.ini file to be present. Most of the time you will
    specify the config explicitly though::

        paster qa update --config=<path to CKAN config file>
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 2
    min_args = 0

    def command(self):
        """
        Parse command line arguments and call appropriate method.
        """
        if not self.args or self.args[0] in ['--help', '-h', 'help']:
            print QACommand.__doc__
            return

        cmd = self.args[0]
        self._load_config()

        # Now we can import ckan and create logger, knowing that loggers
        # won't get disabled
        self.log = logging.getLogger('ckanext.qa')

        log_last_modified = '/var/log/qa-metadata-modified.log'
        log_last_full_run = '/var/log/qa-last-full-run.log'

        if cmd == 'collect-ids':

            # quit if log_last_full_run is with 30 days
            last_run = None
            if os.path.isfile(log_last_full_run):
                with open(log_last_full_run, 'r') as f:
                    last_run = f.readline()

            if last_run:
                import dateutil.parser
                try:
                    last_run_timestamp = dateutil.parser.parse(last_run)
                except ValueError:
                    pass
                else:
                    diff = datetime.datetime.now() - last_run_timestamp
                    if diff.days < 30:
                        raise Exception('Last qa full update was run %s days ago' % diff.days)
                        return

            url = config.get('solr_url') + "/select?q=metadata_modified:[2012-01-01T00:00:000Z%20TO%20NOW]&sort=metadata_modified+asc%2C+id+asc&wt=json&indent=true&fl=id,metadata_modified"
            response = self.get_data(url)
            if response == 'error':
                self.log.error('Error getting response from solr.')
                return

            f = response.read()
            data = json.loads(f)
            total = int(data.get('response').get('numFound'))

            start = 0
            chunk_size = 1000
            counter = start

            sql = '''DELETE FROM qa_ids; ALTER SEQUENCE qa_ids_id_seq RESTART WITH 1;'''
            model.Session.execute(sql)
            model.Session.commit()

            for x in range(0, int(math.ceil(total/chunk_size))+1):
                self.log.info('Collecting %s package ids starting from %s of %s.' %
                    (chunk_size, start, total))
                response = self.get_data(url + "&rows=%s&start=%s" % (chunk_size, start))
                f = response.read()
                data = json.loads(f)
                total = int(data.get('response').get('numFound'))
                results = data.get('response').get('docs')
                metadata_modified_start = None
                metadata_modified_end = None

                for j in range(0, len(results)):
                    if not metadata_modified_start:
                        metadata_modified_start = results[j]['metadata_modified']
                    sql = '''INSERT INTO qa_ids (id, pkg_id, status) VALUES (DEFAULT, :pkg_id, 'New');'''
                    model.Session.execute(sql, {'pkg_id' : results[j]['id']})
                model.Session.commit()

                metadata_modified_end = results[j]['metadata_modified']

                self.log.info('%s package ids collected. metadata_modified starts from %s, ends at %s.' %
                    (len(results), metadata_modified_start, metadata_modified_end))

                start = start + chunk_size

            if os.path.isfile(log_last_modified):
                with open(log_last_modified, 'r+') as f:
                    current = f.readline()
                    if metadata_modified_end > current:
                        f.seek(0)
                        f.write(metadata_modified_end)
                        f.truncate()
            else:
                with open(log_last_modified, 'w+') as f:
                    f.write(metadata_modified_end)
                    f.truncate()

            return

        if cmd == 'update':

            if len(self.args) > 1:
                self.update_resource_rating()
                return

            sql = '''UPDATE qa_ids SET status = 'Running' WHERE id = (SELECT id FROM qa_ids WHERE status = 'New' ORDER BY random() LIMIT 1) RETURNING id, pkg_id;'''
            result = model.Session.execute(sql).fetchall()
            model.Session.commit()
            while result:
                id, pkg_id = result[0]
                self.args.append(pkg_id)
                self.update_resource_rating()
                self.args.pop()
                sql_delete = '''DELETE FROM qa_ids WHERE id = :id'''
                model.Session.execute(sql_delete, {'id' : id})
                model.Session.commit()

                result = model.Session.execute(sql).fetchall()
                model.Session.commit()

            # one more try to make sure it finishes its job
            sql = '''SELECT 1 FROM qa_ids WHERE status = 'New' LIMIT 1;'''
            result = model.Session.execute(sql).fetchone()
            if result:
                raise Exception('qa update job quits early for unknown reason.')
                return

            sql = '''SELECT COUNT(*) FROM qa_ids;'''
            result = model.Session.execute(sql).fetchone()
            if result and result[0]:
                sql = '''UPDATE qa_ids SET status = 'New';'''
                model.Session.execute(sql)
                model.Session.commit()
                self.log.info('qa update thread done. Reset %s Running ones on exit.' %
                    result)
            else:
                self.log.info('qa update thread done. All datasets completed.')
                with open(log_last_full_run, 'w+') as f:
                    f.write(datetime.datetime.now().isoformat())
                    f.truncate()
                from tasks import RemoteResource
                RemoteResource.clear_url_blacklist()

            return

        elif cmd == 'update_sel':
        
             if len(self.args) > 1:
               self.update_resource_rating()
               return

             # create log_last_modified file if the command is being run for the first time
             if not os.path.isfile(log_last_modified):
               try:
                 f = open(log_last_modified, 'w')
                 f.write('1970-01-01T00:00:00.000Z')
                 f.close()
               except IOError as e:
                 print "I/O error({0}): {1}".format(e.errno, e.strerror)

             if os.path.isfile(log_last_modified) and os.stat(log_last_modified).st_size > 1:
                with open(log_last_modified, 'r') as f:
                  for line in f:
                     line = line.replace('\n', '')
                     if not 'Z' in line:
                         last_updated = line + 'Z'
                     else:
                         last_updated = line 
                f.close()
            
                print "Last Updated from file: " + last_updated
            
                url = config.get('solr_url') + "/select?q=metadata_modified:[" + last_updated + "%20TO%20NOW]&sort=metadata_modified+asc%2C+id+asc&wt=json&indent=true&fl=name,metadata_modified"

                response = self.get_data(url)
                
                if (response != 'error'):
                   f = response.read()
                   data = json.loads(f)
                   rows = int(data.get('response').get('numFound'))

                   chunk_size = 1000
                
                   counter = 0
                   start = 0
                   
                   for x in range(0, int(math.ceil(rows/chunk_size))+1):                      

                      print url + "&rows=" + str(chunk_size) + "&start=" + str(start)
                      response = self.get_data(url + "&rows=" + str(chunk_size) + "&start=" + str(start))
                      f = response.read()
                      data = json.loads(f)
                      results = data.get('response').get('docs')

                      for j in range(0, len(results)):
                        print "Currently scanning dataset: " +  results[j]['name'] + " with modified date: " + results[j]['metadata_modified']
                
                        if results[j]['metadata_modified'] != None:
                            fo = open(log_last_modified, "wb")
                            fo.write( str(results[j]['metadata_modified']).strip() )
                            fo.close()
                  
                        self.args.append(results[j]['name'])
                        self.update_resource_rating()
                        self.args.pop()
                        
                        counter = int(counter) + 1
                    
                   start = int(start) + len(results)
                
                print "All Dataset scanned for selective QA update!!"
                
             else:
               print "File for selective update is missing. Run QA update cron first."
               
        elif cmd == 'clean':
            self.log.error('Command "%s" not implemented' % (cmd,))

        else:
            self.log.error('Command "%s" not recognized' % (cmd,))

    def get_data(self, url):
        req = Request(url)
        try:
          response = urlopen(req)
        except HTTPError as e:
          print 'The server couldn\'t fulfill the request.'
          print 'Error code: ', e.code
          return 'error'
        except URLError as e:
          print 'We failed to reach a server.'
          print 'Reason: ', e.reason
          return 'error'
        else:
          return response

    def update_resource_rating(self):

        from ckan import model
        from ckan.model.types import make_uuid

        # import tasks after load config so CKAN_CONFIG evironment variable
        # can be set
        import tasks

        user = p.toolkit.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )
        context = json.dumps({
            'site_url': config['ckan.site_url'],
            'apikey': user.get('apikey'),
            'username': user.get('name'),
        })

        for package in self._package_list():
            self.log.info("QA on dataset being added to Celery queue: %s (%d resources)" %
                                (package.get('name'), len(package.get('resources', []))))

            for resource in package.get('resources', []):
                resource['package'] = package['name']
                pkg = model.Package.get(package['id'])
                if pkg:
                  resource['is_open'] = pkg.isopen()
                  data = json.dumps(resource)
                  task_id = make_uuid()
                  task_status = {
                      'entity_id': resource['id'],
                      'entity_type': u'resource',
                      'task_type': u'qa',
                      'key': u'celery_task_id',
                      'value': task_id,
                      'error': u'',
                      'last_updated': datetime.datetime.now().isoformat()
                  }
                  task_context = {
                      'model': model,
                      'user': user.get('name')
                  }
                  self.log.info("Scanning resource having id: " + resource['id'] + " and name: " + resource['name'])
                  p.toolkit.get_action('task_status_update')(task_context, task_status)
                  tasks.update(context, data)

    def make_post(self, url, data):
            headers = {'Content-type': 'application/json',
                       'Accept': 'text/plain'}
            return requests.post(url, data=json.dumps(data), headers=headers)

    def get_response(self, url, data):
        response = json.loads(requests.get(url, params=data).text)
        return response

    def _package_list(self):
        """
        Generate the package dicts as declared in self.args.

        Make API calls for the packages declared in self.args, and generate
        the package dicts.

        If no packages are declared in self.args, then retrieve all the
        packages from the catalogue.
        """
        api_url = urlparse.urljoin(config['ckan.site_url'], 'api/action')
        if len(self.args) > 1:
            for id in self.args[1:]:
                data = {'id': unicode(id)}
                url = api_url + '/package_show'

                response = self.get_response(url, data)
                if not response.get('success'):
                    err = ('Failed to get package %s from url %r: %s' %
                           (id, url, response.get('error')))
                    self.log.error(err)
                    return

                yield response.get('result')
        else:
            page, limit = 0, 100
            url = api_url + '/current_package_list_with_resources'
            response = self.get_response(url, {'start': page, 'rows': limit})

            if not response.get('success'):
                err = ('Failed to get package list with resources from url %r: %s' %
                       (url, response.get('error')))
                self.log.error(err)
            chunk = response.get('result').get('results')
            while(chunk):
                page = page + limit
                for p in chunk:
                    yield p
                url = api_url + '/current_package_list_with_resources'
                response = self.get_response(url, {'start': page, 'rows': limit})

                try:
                    data = {'start': page, 'rows': limit}
                    r = requests.get(url, params=data)
                    r.raise_for_status()
                except requests.exceptions.RequestException, e:
                    err = ('Failed to get package list with resources from url %r: %s' %
                       (url, str(e)))
                    self.log.error(err)
                    continue

                chunk = response.get('result').get('results')
