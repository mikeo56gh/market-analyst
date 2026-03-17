import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

EIA_API_KEY = os.getenv('EIA_API_KEY', '')
ENTSOE_API_KEY = os.getenv('ENTSOE_API_KEY', '')
NEWSAPI_KEY = os.getenv('NEWSAPI_KEY', '')
GIE_API_KEY = os.getenv('GIE_API_KEY', '')
OUTPUT_DIR = os.getenv('OUTPUT_DIR', '.')
TIMEZONE = os.getenv('REPORT_TIMEZONE', 'Europe/London')

REPORT_DATE = datetime.now(timezone.utc).date().isoformat()
REPORT_TS = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
WINDOW_START = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
OUTPUT_PATH = os.path.join(OUTPUT_DIR, f'data_pack_{REPORT_DATE}.json')
LATEST_PATH = os.path.join(OUTPUT_DIR, 'latest.json')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger('data_pack')

SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'market-dashboard/1.0'})


def safe_get_json(url: str, *, params: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = 25, label: str = '') -> Optional[dict]:
    try:
        r = SESSION.get(url, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning('[%s] json fetch failed: %s', label, e)
        return None


def safe_get_text(url: str, *, params: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = 25, label: str = '') -> Optional[str]:
    try:
        r = SESSION.get(url, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception as e:
        log.warning('[%s] text fetch failed: %s', label, e)
        return None


def null_value(field: str, unit: str, source: str, note: str = '') -> dict:
    out = {field: None, 'unit': unit, 'source': source, 'data_quality': 'unavailable'}
    if note:
        out['note'] = note
    return out


def maybe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ''):
            return None
        return float(value)
    except Exception:
        return None


def fetch_gas_storage() -> List[dict]:
    url = 'https://agsi.gie.eu/api'
    headers = {'Accept': 'application/json'}
    if GIE_API_KEY:
        headers['x-key'] = GIE_API_KEY

    results: List[dict] = []
    for country, label in [('eu', 'EU'), ('gb', 'UK')]:
        data = safe_get_json(url, params={'country': country, 'size': 1}, headers=headers, label=f'GIE-{label}')
        if data and data.get('data'):
            row = data['data'][0]
            ts = row.get('gasDayStart') or REPORT_DATE
            if len(ts) == 10:
                ts += 'T00:00:00Z'
            results.append({
                'region': label,
                'metric': 'storage_level',
                'value': maybe_float(row.get('full')),
                'unit': '%',
                'timestamp': ts,
                'source': 'GIE_AGSI_plus',
            })
        else:
            note = 'Set GIE_API_KEY if your AGSI access requires a personal API key.' if not GIE_API_KEY else 'No data returned.'
            results.append({
                'region': label,
                'metric': 'storage_level',
                'value': None,
                'unit': '%',
                'timestamp': REPORT_TS,
                'source': 'GIE_AGSI_plus',
                'data_quality': 'unavailable',
                'note': note,
            })
    return results


def fetch_entsog_flows() -> List[dict]:
    url = 'https://transparency.entsog.eu/api/v1/operationalData'
    params = {
        'forceDownload': 'true',
        'pointDirection': 'be-tso-0001itp-00080exit,be-tso-0001itp-00080entry',
        'indicator': 'Nomination',
        'periodType': 'day',
        'timezone': 'UTC',
        'limit': 2,
        'dataset': 1,
    }
    data = safe_get_json(url, params=params, label='ENTSOG-IUK')
    rows = []
    for row in (data or {}).get('operationalData', []):
        direction = row.get('directionKey', 'unknown')
        rows.append({
            'point_name': row.get('pointLabel', 'Interconnector UK'),
            'country_from': 'GB' if direction == 'exit' else 'BE',
            'country_to': 'BE' if direction == 'exit' else 'GB',
            'flow_direction': direction,
            'value': maybe_float(row.get('value')),
            'unit': row.get('unit', 'kWh/d'),
            'timestamp': row.get('periodFrom', REPORT_TS),
            'source': 'ENTSOG',
        })
    return rows or [{
        'point_name': 'Interconnector UK', 'country_from': 'GB', 'country_to': 'BE', 'flow_direction': 'unknown',
        'value': None, 'unit': 'kWh/d', 'timestamp': REPORT_TS, 'source': 'ENTSOG', 'data_quality': 'unavailable'
    }]


def fetch_entsog_lng() -> List[dict]:
    url = 'https://transparency.entsog.eu/api/v1/operationalData'
    params = {
        'forceDownload': 'true',
        'indicator': 'Sendout',
        'periodType': 'day',
        'timezone': 'UTC',
        'limit': 10,
        'dataset': 1,
        'pointDirection': 'uk-tso-0001itp-00349exit,uk-tso-0001itp-00350exit,uk-tso-0001itp-00348exit',
    }
    data = safe_get_json(url, params=params, label='ENTSOG-LNG')
    rows = []
    for row in (data or {}).get('operationalData', []):
        rows.append({
            'terminal': row.get('pointLabel', 'UK LNG terminal'),
            'metric': 'sendout',
            'value': maybe_float(row.get('value')),
            'unit': row.get('unit', 'kWh/d'),
            'timestamp': row.get('periodFrom', REPORT_TS),
            'source': 'ENTSOG',
        })
    return rows or [{
        'terminal': 'UK_LNG_terminals', 'metric': 'sendout', 'value': None, 'unit': 'kWh/d',
        'timestamp': REPORT_TS, 'source': 'ENTSOG', 'data_quality': 'unavailable'
    }]


def fetch_carbon_intensity() -> List[dict]:
    data = safe_get_json('https://api.carbonintensity.org.uk/intensity', label='NESO-Carbon')
    row = ((data or {}).get('data') or [{}])[0]
    intensity = row.get('intensity', {})
    value = intensity.get('actual', intensity.get('forecast'))
    return [{
        'metric': 'carbon_intensity_actual_or_forecast',
        'region': 'GB',
        'value': value,
        'unit': 'gCO2/kWh',
        'timestamp': row.get('from', REPORT_TS),
        'source': 'NESO_Carbon_Intensity',
        'index': intensity.get('index', 'unknown'),
    }]


def fetch_brent() -> List[dict]:
    if not EIA_API_KEY:
        return [{**null_value('value', 'USD/bbl', 'EIA'), 'metric': 'brent', 'timestamp': REPORT_TS, 'note': 'Set EIA_API_KEY.'}]
    data = safe_get_json(
        'https://api.eia.gov/v2/petroleum/pri/spt/data/',
        params={
            'api_key': EIA_API_KEY,
            'frequency': 'daily',
            'data[0]': 'value',
            'facets[product][]': 'EPCBRENT',
            'sort[0][column]': 'period',
            'sort[0][direction]': 'desc',
            'length': 1,
        },
        label='EIA-Brent',
    )
    row = ((data or {}).get('response') or {}).get('data', [])
    if row:
        row = row[0]
        return [{
            'metric': 'brent',
            'value': maybe_float(row.get('value')),
            'unit': 'USD/bbl',
            'timestamp': f"{row.get('period', REPORT_DATE)}T00:00:00Z",
            'source': 'EIA',
        }]
    return [{**null_value('value', 'USD/bbl', 'EIA'), 'metric': 'brent', 'timestamp': REPORT_TS}]


def _strip_ns(tag: str) -> str:
    return tag.split('}', 1)[-1]


def _parse_entsoe_quantity(xml_text: str) -> Optional[float]:
    root = ET.fromstring(xml_text)
    quantities: List[float] = []
    for elem in root.iter():
        if _strip_ns(elem.tag) == 'quantity' and elem.text:
            val = maybe_float(elem.text)
            if val is not None:
                quantities.append(val)
    return quantities[0] if quantities else None


def fetch_entsoe_flows() -> List[dict]:
    if not ENTSOE_API_KEY:
        return [
            {'metric': 'cross_border_flow', 'region': 'FR-GB', 'value': None, 'unit': 'MW', 'timestamp': REPORT_TS, 'source': 'ENTSO-E', 'data_quality': 'unavailable', 'note': 'Set ENTSOE_API_KEY.'},
            {'metric': 'cross_border_flow', 'region': 'NL-GB', 'value': None, 'unit': 'MW', 'timestamp': REPORT_TS, 'source': 'ENTSO-E', 'data_quality': 'unavailable', 'note': 'Set ENTSOE_API_KEY.'},
        ]
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    pairs = [
        ('10YFR-RTE------C', '10YGB----------A', 'FR-GB'),
        ('10YNL----------L', '10YGB----------A', 'NL-GB'),
    ]
    rows = []
    for in_domain, out_domain, label in pairs:
        xml_text = safe_get_text(
            'https://web-api.tp.entsoe.eu/api',
            params={
                'securityToken': ENTSOE_API_KEY,
                'documentType': 'A11',
                'in_Domain': in_domain,
                'out_Domain': out_domain,
                'periodStart': now.strftime('%Y%m%d%H00'),
                'periodEnd': (now + timedelta(hours=1)).strftime('%Y%m%d%H00'),
            },
            headers={'Accept': 'application/xml,text/xml;q=0.9,*/*;q=0.8'},
            label=f'ENTSOE-{label}',
        )
        val = None
        note = ''
        if xml_text:
            try:
                val = _parse_entsoe_quantity(xml_text)
            except Exception as e:
                note = f'XML parse error: {e}'
        rows.append({
            'metric': 'cross_border_flow',
            'region': label,
            'value': val,
            'unit': 'MW',
            'timestamp': REPORT_TS,
            'source': 'ENTSO-E',
            **({'data_quality': 'unavailable', 'note': note or 'No quantity returned.'} if val is None else {}),
        })
    return rows


def fetch_imf_proxies() -> List[dict]:
    data = safe_get_json('https://www.imf.org/external/datamapper/api/v1/PNGASEU,PFERT', label='IMF')
    values = (data or {}).get('values', {})
    rows = []
    label_map = {'PNGASEU': ('eu_gas_proxy', 'EUR/MBtu_proxy'), 'PFERT': ('ammonia_fertiliser_proxy', 'USD/mt_proxy')}
    for series_id, series in values.items():
        if not series:
            continue
        latest_period = sorted(series.keys())[-1]
        name, unit = label_map.get(series_id, (series_id, 'index'))
        rows.append({
            'product_or_chain': name,
            'proxy_name': f'IMF_{series_id}',
            'value': maybe_float(series[latest_period]),
            'unit': unit,
            'timestamp': f'{latest_period}-01-01T00:00:00Z',
            'source': 'IMF_Datamapper',
            'is_proxy': True,
            'note': 'Monthly series. Use as structural benchmark only.',
        })
    return rows or [{
        'product_or_chain': 'eu_gas_and_ammonia_proxies', 'proxy_name': 'IMF_PNGASEU_PFERT', 'value': None,
        'unit': 'index', 'timestamp': REPORT_TS, 'source': 'IMF_Datamapper', 'is_proxy': True, 'data_quality': 'unavailable'
    }]


WATCH_QUERIES = [
    ('UK gas price', 'energy'),
    ('ethylene outage', 'chemicals'),
    ('LNG Europe', 'energy'),
    ('hydrogen UK', 'new_energies'),
    ('CCS carbon capture', 'new_energies'),
    ('SAF sustainable aviation fuel', 'new_energies'),
]


def fetch_news() -> List[dict]:
    if not NEWSAPI_KEY:
        return []
    rows: List[dict] = []
    seen = set()
    for query, category in WATCH_QUERIES[:5]:
        data = safe_get_json(
            'https://newsapi.org/v2/everything',
            params={
                'q': query,
                'sortBy': 'publishedAt',
                'language': 'en',
                'pageSize': 3,
                'from': (datetime.now(timezone.utc) - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'apiKey': NEWSAPI_KEY,
            },
            label=f'NewsAPI-{query[:20]}',
        )
        for i, art in enumerate((data or {}).get('articles', [])):
            url = art.get('url', '')
            if not url or url in seen:
                continue
            seen.add(url)
            rows.append({
                'id': f'news_{len(rows)+1:03d}',
                'published_at': art.get('publishedAt', REPORT_TS),
                'headline': art.get('title', ''),
                'summary': art.get('description', ''),
                'category': category,
                'region': 'Global',
                'source': art.get('source', {}).get('name', 'NewsAPI'),
                'url': url,
                'relevance_score': max(1, 5 - i),
                'potential_uk_energy_impact': 'medium',
            })
    return rows


def build_manual_stubs() -> Dict[str, Any]:
    return {
        'uk_nbp': [{'timestamp': REPORT_TS, 'price': None, 'unit': 'GBP/therm', 'source': 'manual_required', 'note': 'Read from ICE or broker screen.'}],
        'eu_ttf': [{'timestamp': REPORT_TS, 'price': None, 'unit': 'EUR/MWh', 'source': 'manual_required', 'note': 'Read from ICE or broker screen.'}],
        'eu_ets_carbon': [{'metric': 'eu_ets_proxy', 'value': None, 'unit': 'EUR/tCO2', 'timestamp': REPORT_TS, 'source': 'manual_required', 'note': 'Read from Ember or ICE EUA futures.'}],
        'ethylene_europe': [{'product': 'ethylene', 'region': 'Europe', 'price': None, 'unit': 'EUR/mt', 'timestamp': REPORT_TS, 'source': 'manual_required', 'note': 'No reliable free API.'}],
        'naphtha_proxy': [{'name': 'naphtha_proxy', 'value': None, 'unit': 'USD/mt', 'timestamp': REPORT_TS, 'source': 'proxy_from_brent', 'note': 'Estimate from Brent x 7.45 - 50, or overwrite manually.'}],
    }


def build_pack() -> Dict[str, Any]:
    stubs = build_manual_stubs()
    pack = {
        'report_metadata': {
            'report_date': REPORT_DATE,
            'report_time_utc': REPORT_TS,
            'timezone': TIMEZONE,
            'coverage_window': {'start': WINDOW_START, 'end': REPORT_TS},
            'data_quality_note': 'NBP, TTF, EU ETS, ethylene and REMIT still need manual support.',
        },
        'market_context': {
            'uk_focus': True,
            'regions': ['UK', 'Europe', 'Global'],
            'site_profile': {
                'exposures': ['natural_gas', 'power', 'steam', 'industrial_chemicals', 'utilities', 'logistics', 'decarbonisation_projects'],
                'watch_topics': ['plant_upsets_uk', 'energy_price_shocks', 'europe_chemical_news', 'saf', 'hydrogen', 'ammonia', 'ccs'],
            },
        },
        'energy': {
            'gas': {
                'uk_nbp': stubs['uk_nbp'],
                'eu_ttf': stubs['eu_ttf'],
                'entsog_flows': fetch_entsog_flows(),
                'lng': fetch_entsog_lng(),
                'storage': fetch_gas_storage(),
            },
            'power': {
                'gb_system_context': fetch_carbon_intensity(),
                'entsoe_context': fetch_entsoe_flows(),
                'oil_and_macro_proxies': fetch_brent(),
                'carbon': stubs['eu_ets_carbon'],
            },
        },
        'chemicals': {
            'direct_prices': stubs['ethylene_europe'],
            'proxy_series': fetch_imf_proxies(),
            'feedstocks': stubs['naphtha_proxy'],
            'margin_flags': [{'chain': 'olefins', 'status': 'unknown', 'reason': 'Ethylene and naphtha need manual support.', 'confidence': 'Low'}],
        },
        'plant_disruptions': [{
            'event_id': 'remit_manual_check', 'date_detected': REPORT_TS, 'site_name': 'Manual check required',
            'country': 'EU/UK', 'sector': 'all', 'event_type': 'manual_check', 'status': 'pending',
            'description': 'Check ACER REMIT, ENTSO-E outage messages, and NESO market notices manually.',
            'source': 'manual', 'supply_chain_relevance': 'high', 'likely_duration': 'unknown', 'confidence': 'Manual check required'
        }],
        'news_signals': fetch_news(),
        'new_energies': {
            'saf': {'projects': [], 'policy': []},
            'hydrogen': {'projects': [], 'cost_proxies': []},
            'ammonia': {'projects': [], 'price_proxies': []},
            'ccs': {'projects': [], 'policy_support': []},
        },
        'manual_analyst_notes': [{'note': 'Enter manual prices before using this pack for the final brief.', 'priority': 'high'}],
    }

    brent = (((pack.get('energy') or {}).get('power') or {}).get('oil_and_macro_proxies') or [{}])[0].get('value')
    if brent is not None:
        pack['chemicals']['feedstocks'][0]['value'] = round(brent * 7.45 - 50, 1)
        pack['chemicals']['feedstocks'][0]['source'] = 'proxy_from_EIA_brent'
    return pack


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pack = build_pack()
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(pack, f, indent=2)
    with open(LATEST_PATH, 'w', encoding='utf-8') as f:
        json.dump(pack, f, indent=2)
    log.info('Wrote %s and %s', OUTPUT_PATH, LATEST_PATH)


if __name__ == '__main__':
    main()
