from collections import Counter
import copy
try:
    from collections import OrderedDict  # from python 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from ckan.common import _
import ckan.model as model
import ckan.plugins as p
from ckanext.report import lib

import logging

log = logging.getLogger(__name__)

def five_stars(id=None):
    """
    Return a list of dicts: 1 for each dataset that has an openness score.
    Each dict is of the form:
        {'name': <string>, 'title': <string>, 'openness_score': <int>}
    """
    if id:
        pkg = model.Package.get(id)
        if not pkg:
            return "Not found"

    # take the maximum openness score among dataset resources to be the
    # overall dataset openness core
    query = model.Session.query(model.Package.name, model.Package.title,
                                model.Resource.id,
                                model.TaskStatus.value.label('value'))
    query = _join_package_to_resource_group_if_it_exists(query)
    query = query \
        .join(model.Resource)\
        .join(model.TaskStatus, model.TaskStatus.entity_id == model.Resource.id)\
        .filter(model.TaskStatus.key==u'openness_score')\
        .group_by(model.Package.name, model.Package.title, model.Resource.id, model.TaskStatus.value)\
        .distinct()

    if id:
        query = query.filter(model.Package.id == pkg.id)

    results = []
    for row in query:
        results.append({
            'name': row.name,
            'title': row.title + u' ' + row.id,
            'openness_score': row.value
        })

    return results


def resource_five_stars(id):
    """
    Return a dict containing the QA results for a given resource
    Each dict is of the form:
        {'openness_score': <int>, 'openness_score_reason': <string>, 'failure_count': <int>}
    """
    if id:
        r = model.Resource.get(id)
        if not r:
            return {}  # Not found

    context = {'model': model, 'session': model.Session}
    data = {'entity_id': r.id, 'task_type': 'qa'}

    try:
        data['key'] = 'openness_score'
        status = p.toolkit.get_action('task_status_show')(context, data)
        openness_score = int(status.get('value'))
        openness_score_updated = status.get('last_updated')

        data['key'] = 'openness_score_reason'
        status = p.toolkit.get_action('task_status_show')(context, data)
        openness_score_reason = status.get('value')
        openness_score_reason_updated = status.get('last_updated')

        data['key'] = 'openness_score_failure_count'
        status = p.toolkit.get_action('task_status_show')(context, data)
        openness_score_failure_count = int(status.get('value'))
        openness_score_failure_count_updated = status.get('last_updated')

        last_updated = max( 
            openness_score_updated,
            openness_score_reason_updated,
            openness_score_failure_count_updated )

        result = {
            'openness_score': openness_score,
            'openness_score_reason': openness_score_reason,
            'openness_score_failure_count': openness_score_failure_count,
            'openness_score_updated': openness_score_updated,
            'openness_score_reason_updated': openness_score_reason_updated,
            'openness_score_failure_count_updated': openness_score_failure_count_updated,
            'openness_updated': last_updated
        }
    except p.toolkit.ObjectNotFound:
        result = {}

    return result

def openness_report(organization, include_sub_organizations=False):
    if organization is None:
        return openness_index(include_sub_organizations=include_sub_organizations)
    else:
        return openness_for_organization(organization=organization, include_sub_organizations=include_sub_organizations)


def openness_index(include_sub_organizations=False):
    '''Returns the counts of 5 stars of openness for all organizations.'''

    context = {'model': model, 'session': model.Session, 'ignore_auth': True}
    total_score_counts = Counter()
    counts = {}
    # Get all the scores and build up the results by org
    for org in add_progress_bar(model.Session.query(model.Group)
            .filter(model.Group.type == 'organization')
            .filter(model.Group.state == 'active').all()):
        scores = []
        # NB org.packages() misses out many - see:
        # http://redmine.dguteam.org.uk/issues/1844
        pkgs = model.Session.query(model.Package) \
                    .filter_by(owner_org=org.id) \
                    .filter_by(state='active') \
                    .all()
        for pkg in pkgs:
            try:
                qa = p.toolkit.get_action('qa_package_openness_show')(context, {'id': pkg.id})
            except p.toolkit.ObjectNotFound:
                log.warning('No QA info for package %s', pkg.name)
                return
            scores.append(qa['openness_score'])
        score_counts = Counter(scores)
        total_score_counts += score_counts
        counts[org.name] = {
            'organization_title': org.title,
            'score_counts': score_counts,
        }

    counts_with_sub_orgs = copy.deepcopy(counts)  # new dict
    if include_sub_organizations:
        for org_name in counts_with_sub_orgs:
            org = model.Group.by_name(org_name)

            for sub_org_id, sub_org_name, sub_org_title, sub_org_parent_id \
                    in org.get_children_group_hierarchy(type='organization'):
                if sub_org_name not in counts:
                    # occurs only if there is an organization created since the last loop?
                    continue
                counts_with_sub_orgs[org_name]['score_counts'] += \
                        counts[sub_org_name]['score_counts']
        results = counts_with_sub_orgs
    else:
        results = counts

    table = []
    for org_name, org_counts in results.iteritems():
        total_stars = sum([k*v for k, v in org_counts['score_counts'].items() if k])
        num_pkgs_scored = sum([v for k, v in org_counts['score_counts'].items()
                              if k is not None])
        average_stars = round(float(total_stars) / num_pkgs_scored, 1) \
                        if num_pkgs_scored else 0.0
        row = OrderedDict((
            ('organization_title', results[org_name]['organization_title']),
            ('organization_name', org_name),
            ('total_stars', total_stars),
            ('average_stars', average_stars),
            ))
        row.update(jsonify_counter(org_counts['score_counts']))
        table.append(row)

    table.sort(key=lambda x: (-x['total_stars'],
                              -x['average_stars']))

    # Get total number of packages & resources
    num_packages = model.Session.query(model.Package)\
                        .filter_by(state='active')\
                        .count()
    return {'table': table,
            'total_score_counts': jsonify_counter(total_score_counts),
            'num_packages_scored': sum(total_score_counts.values()),
            'num_packages': num_packages,
            }

def openness_for_organization(organization=None, include_sub_organizations=False):
    org = model.Group.get(organization)
    if not org:
        raise p.toolkit.ObjectNotFound

    if not include_sub_organizations:
        orgs = [org]
    else:
        orgs = lib.go_down_tree(org)

    context = {'model': model, 'session': model.Session, 'ignore_auth': True}
    score_counts = Counter()
    rows = []
    num_packages = 0
    for org in orgs:
        # NB org.packages() misses out many - see:
        # http://redmine.dguteam.org.uk/issues/1844
        pkgs = model.Session.query(model.Package) \
                    .filter_by(owner_org=org.id) \
                    .filter_by(state='active') \
                    .all()
        num_packages += len(pkgs)
        for pkg in pkgs:
            try:
                qa = p.toolkit.get_action('qa_package_openness_show')(context, {'id': pkg.id})
            except p.toolkit.ObjectNotFound:
                log.warning('No QA info for package %s', pkg.name)
                return
            rows.append(OrderedDict((
                ('dataset_name', pkg.name),
                ('dataset_title', pkg.title),
                ('dataset_notes', lib.dataset_notes(pkg)),
                ('organization_name', org.name),
                ('organization_title', org.title),
                ('openness_score', qa['openness_score']),
                ('openness_score_reason', qa['openness_score_reason']),
                )))
            score_counts[qa['openness_score']] += 1

    total_stars = sum([k*v for k, v in score_counts.items() if k])
    num_pkgs_with_stars = sum([v for k, v in score_counts.items()
                               if k is not None])
    average_stars = round(float(total_stars) / num_pkgs_with_stars, 1) \
                    if num_pkgs_with_stars else 0.0

    return {'table': rows,
            'score_counts': jsonify_counter(score_counts),
            'total_stars': total_stars,
            'average_stars': average_stars,
            'num_packages_scored': len(rows),
            'num_packages': num_packages,
            }


def openness_report_combinations():
    for organization in lib.all_organizations(include_none=True):
        for include_sub_organizations in (False, True):
            yield {'organization': organization,
                   'include_sub_organizations': include_sub_organizations}


openness_report_info = {
    'name': 'openness',
    'title': _('Openness (Five Stars)'),
    'description': _('Datasets graded on Tim Berners Lees\' Five Stars of Openness - openly licensed, openly accessible, structured, open format, URIs for entities, linked.'),
    'option_defaults': OrderedDict((('organization', None),
                                    ('include_sub_organizations', False),
                                    )),
    'option_combinations': openness_report_combinations,
    'generate': openness_report,
    'template': 'report/openness.html',
    }


def jsonify_counter(counter):
    # When counters are stored as JSON, integers become strings. Do the conversion
    # here to ensure that when you run the report the first time, you get the same
    # response as subsequent times that go through the cache/JSON.
    return dict((str(k) if k is not None else k, v) for k, v in counter.items())


def add_progress_bar(iterable, caption=None):
    try:
        # Add a progress bar, if it is installed
        import progressbar
        bar = progressbar.ProgressBar(widgets=[
            (caption + ' ') if caption else '',
            progressbar.Percentage(), ' ',
            progressbar.Bar(), ' ', progressbar.ETA()])
        return bar(iterable)
    except ImportError:
        return iterable
