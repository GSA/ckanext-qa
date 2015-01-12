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


REQUESTS_HEADER = {'content-type': 'application/json'}

class CkanApiError(Exception):
    pass


class QACommand(p.toolkit.CkanCommand):
    """
    QA analysis of CKAN resources

    Usage::

        paster qa [options] update [dataset name/id]
           - QA analysis on all resources in a given dataset, or on all
           datasets if no dataset given

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

        if cmd == 'update':
            self.update_resource_rating()

        elif cmd == 'update_sel':
            from ckan import model

            if os.path.isfile('/var/log/ckan_qa_date_log.txt') and os.stat("/var/log/ckan_qa_date_log.txt").st_size > 1:
                
                i = 0
                with open('/var/log/ckan_qa_date_log.txt', 'r') as f:
                  for line in f:
                     if i == 0:
                       line = line.replace('\n', '')
                       if not 'Z' in line:
                          last_updated = line + 'Z'
                       else:
                          last_updated = line 
                     else:
                       start = line
                     
                     i = i + 1
                f.close()    
            else:
                last_updated = '2012-01-01T00:00:000Z'
                start = 0
            
            print "Last Updated from file: " + last_updated
            
            
            url = config.get('solr_url') + "/select?q=metadata_modified:[" + last_updated + "%20TO%20NOW]&sort=metadata_modified+asc%2C+id+asc&wt=json&indent=true&fl=name,metadata_modified"


            response = self.get_data(url)

            if (response != 'error'):
                f = response.read()
                data = json.loads(f)
                rows = int(data.get('response').get('numFound')) - int(start)

                chunk_size = 1000
                
                counter = start

                for x in range(0, int(math.ceil(rows/chunk_size))+1):                      

                    print url + "&rows=" + str(chunk_size) + "&start=" + str(start)
                    response = self.get_data(url + "&rows=" + str(chunk_size) + "&start=" + str(start))
                    f = response.read()
                    data = json.loads(f)
                    results = data.get('response').get('docs')

                    for j in range(0, len(results)):
                        print "Currently scanning dataset: " +  results[j]['name'] + " with modified date: " + results[j]['metadata_modified']
                
                        if results[j]['metadata_modified'] != None:
                            fo = open("/var/log/ckan_qa_date_log.txt", "wb")
                            fo.write( str(results[j]['metadata_modified']).strip() + "\n")
                            
                            '''if x == int(math.ceil(rows/chunk_size)):
                               fo.write("0")
                            else:'''   
                            fo.write(str(counter))
                            fo.close()
                  
                        self.args.append(results[j]['name'])
                        self.update_resource_rating()
                        self.args.pop()
                        
                        counter = int(counter) + 1
                    
                    start = int(start) + len(results)
                    
                i = 0
                with open('/var/log/ckan_qa_date_log.txt', 'r') as f:
                  for line in f:
                     if i == 0:
                       line = line.replace('\n', '')
                       if not 'Z' in line:
                          last_updated = line + 'Z'
                       else:
                          last_updated = line 
                     i = i + 1
                f.close()    
                
                fo = open("/var/log/ckan_qa_date_log.txt", "wb")
                fo.write( str(last_updated).strip() + "\n")
                fo.write("0")
                fo.close()
                
                print "All Dataset scanned!!"
               
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