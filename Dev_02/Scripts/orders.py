from shared.tools.logging import Logger
from shared.tools.global import ExtraGlobal
from shared.tools.thread import async
from shared.tools.error import python_full_stack

from eurosort.config import EuroSorterConfig
from eurosort.service import EuroSorterPolling, EuroSorterPermissivePolling
from eurosort.tracking.contents import EuroSorterContentTracking, Destination, Sides
from eurosort.tracking.lights import EuroSorterLightControl
from eurosort.tracking.wcs import EuroSorterAccessWCS
from eurosort.enums import MessageCode
from eurosort.helpers.tools import *

from eurosort.utility import now, seconds_since, coerce_to_set
from system.date import now as date_now
from datetime import datetime
import system
import re


get_dims = 'Dims'
get_max = 'Dims/Mode/Max'
get_min = 'Dims/Mode/Min'

# ---------------------------------------------------------------------------
# Regex + classification helpers
# ---------------------------------------------------------------------------

NOREAD_RE  = re.compile(r'^NoRead$')
NOSCAN_RE  = re.compile(r'^NoScanTX$')
INVALID_RE = re.compile(r'^NoCode$')

ERROR_ZONES = ['JACKPOT', 'NOREAD', 'UNRESOLVED']
NODEST = ['!!', '??']

# Tote: RCV###
TOTE_RE = re.compile(r'^RCV\d{3}$')
# DST: DST-0001..9999-1/2-1/2-A/B
DST_RE = re.compile(r'^DST-(?!0000)\d{4}-(1|2)-(1|2)-(A|B)$')
# SDR literal
SDR_RE = re.compile(r'^SDR$')
# IBN: 6 alphanumeric chars, BUT NOT a Tote (RCV###)
IBN_RE = re.compile(r'^(?!RCV\d{3}$)[A-Z0-9]{6}$')

# for Level 3 _route_noread
NOREAD_VALUES = set(['noread'])

error_matches = {
	NOREAD_RE:  "NOREAD",
	NOSCAN_RE:  "NOSCAN",
	INVALID_RE: "NOCODE",
}

code_matches = {
	DST_RE:  "DST",
	TOTE_RE: "TOTE",
	SDR_RE:  "SDR",
	IBN_RE:  "IBN",
}

lifespan = 60 * 60 * 24 * 7  # one week


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(v, default=0.0):
	if v is None:
		return round(float(default), 2)
	try:
		return round(float(v), 2)
	except Exception:
		return round(float(default), 2)


def _volume(l, w, h):
	try:
		return round(float(l) * float(w) * float(h), 2)
	except Exception:
		return 0.0


# ===========================================================================
# LEVEL 3
# ===========================================================================

class Level_3_OrderRouting(
	EuroSorterContentTracking,
	EuroSorterPermissivePolling,
	EuroSorterPolling,
	EuroSorterAccessWCS,
	EuroSorterLightControl,
):
	CONTROL_PERMISSIVE_TAG_MAPPING = {
		'auto_active': 'Auto Unload Active',
		'group_by': 'Group_By',
		'sort_by': 'Sort_By',
		'max_fill': 'Max_Fill',
		'max_noread_recirc': 'No Read recirc attempts',
		'max_resort_recirc': 'recirc attempts',
		'extra_volume': 'Extra_Volume',
		'tote_main_volume': 'Tote_Volume_Overall',
		'tote_use_volume': 'Volume',
		'lane1_enabled': 'Chute_Control/Lane_1_Enabled',
		'lane2_enabled': 'Chute_Control/Lane_2_Enabled',
		'lane3_enabled': 'Chute_Control/Lane_3_Enabled',
		'lane4_enabled': 'Chute_Control/Lane_4_Enabled',
		'lane1_set': 'AutoAssign/Lane_1/Set',
		'lane2_set': 'AutoAssign/Lane_2/Set',
		'lane3_set': 'AutoAssign/Lane_3/Set',
		'lane4_set': 'AutoAssign/Lane_4/Set',
		'clear_all': 'Reset/Clear_all_Data',
		'clear_chute': 'Reset/Clear_Chute_Data',
		'chute_to_delete': 'Reset/Clear_Chute_Data',
		'squelch_WCS': 'Squelch all WCS updates',
	}

	def __init__(self, name, **init_cfg):
		super(Level_3_OrderRouting, self).__init__(name, **init_cfg)

		self.loggerInfo = system.util.getLogger("Level3_info")

		self.scan_counts = {
			'scanner': {'GoodRead': 0, 'NoRead': 0, 'TotalScans': 0, 'Rate': 0.0},
		}
		self.divert_counts = {
			'diverts': {
				'Confirmed': 0,
				'Confirmed_NoRead': 0,
				'Confirmed_Jackpot': 0,
				'Failed_Full': 0,
				'Failed_Wrong': 0,
				'Failed_Other': 0,
				'Total_Discharged': 0,
			}
		}

		# FIX #3: two independent timestamps so each periodic fires on its own
		# cadence without the 60-120s overlap bug
		self._last_check_key_updates = system.date.now()
		self._last_check_door_state  = system.date.now()

		self._polling_methods.append(self._check_processed_chutes_periodic)
		self._polling_methods.append(self._assign_initial_error_chutes)
		self._polling_methods.append(self._get_chute_updates)

		self.last_sorted_lane = 1

		self._router_sequence = [
			self._route_order,
			self._route_noread,
			self._max_recirc,
		]

		for perm, tag in self.CONTROL_PERMISSIVE_TAG_MAPPING.items():
			self._subscribe_control_permissive(perm, tag)

		self._init_polling()

	# -------------------------------------------------------------------------
	# Small utilities
	# -------------------------------------------------------------------------

	def _safe_tag_write(self, paths, values):
		try:
			if isinstance(paths, (str, unicode)):
				paths = [paths]
			if not isinstance(values, (list, tuple)):
				values = [values]
			system.tag.writeBlocking(paths, values)
		except Exception as e:
			try:
				self.logger.error("Tag write failed for {}: {}".format(paths, e))
			except Exception:
				pass

	def _parse_list_field(self, field):
		if not field:
			return []
		if isinstance(field, (list, tuple)):
			return list(field)
		s = str(field).strip()
		if not s:
			return []
		return [x for x in s.split(',') if x != '']

	def _calculate_volume_metrics(self, curr, delta):
		new_vol = float(curr or 0.0) + float(delta or 0.0)
		tote_vol = float(self.get_permissive('tote_main_volume') or 1.0)
		percent = round((new_vol / tote_vol) * 100.0, 2) if tote_vol else 0.0
		return new_vol, percent

	def _calculate_volume_metrics_product(self, l, w, h):
		tote_vol = float(self.get_permissive('tote_main_volume') or 0.0)
		extra = float(self.get_permissive('extra_volume') or 0.0)

		orig = float(l or 0.0) * float(w or 0.0) * float(h or 0.0)
		if orig <= 0:
			return (tote_vol * (extra / 100.0))

		return (orig + (orig * extra / 100.0))

	def _resolve_issue(self, barcode_str):
		codes = str(barcode_str or '').split(',')
		return self.wcs_get_issue(codes)

	# -------------------------------------------------------------------------
	# Destination filtering
	# -------------------------------------------------------------------------

	def _match_cond(self, rec, cond):
		if not isinstance(cond, dict):
			return True

		for k, v in cond.items():
			if k == "$and":
				for sub in (v or []):
					if not self._match_cond(rec, sub):
						return False
				continue

			value = self._dest_get(rec, k, None)

			if isinstance(v, dict) and "$regex" in v:
				pat = v.get("$regex") or ""
				try:
					if re.match(pat, str(value or "")) is None:
						return False
				except Exception:
					return False
				continue

			if value != v:
				return False

		return True

	def _find_destinations(self, filt):
		out = []
		for _, rec in (self._destination_contents or {}).items():
			if not isinstance(rec, dict):
				continue
			if self._match_cond(rec, filt):
				out.append(rec)
		return out

	# -------------------------------------------------------------------------
	# Periodics / status
	# -------------------------------------------------------------------------

	def _get_chute_updates(self):
		try:
			chute_counts = self.get_chutes_updates()
			transit_counts = self.get_transit_updates()
			try:
				chute_counts[0].update(transit_counts[0])
			except Exception:
				pass

			self._safe_tag_write(
				'[EuroSort]EuroSort/Level3/Sorter_Control/Status/Counts',
				chute_counts
			)
		except Exception:
			return

	def _check_door_state(self):
		path_base = '[EuroSort]EuroSort/Level3/Destinations/'
		door_path = 'Destination/Chute_Door_Status'

		chutes = self._find_destinations({"$and": [{"in_service": True}]})
		for chute in chutes:
			destination = chute.get('_id') or chute.get('destination')
			if not destination:
				continue

			tag_path = '{0}{1}/{2}'.format(path_base, destination, door_path)
			exec_path = tag_path + '.Executed'

			try:
				door_status, exec_ = [
					qv.value for qv in system.tag.readBlocking([tag_path, exec_path])
				]
			except Exception:
				continue

			door_status = bool(door_status)
			exec_ = bool(exec_)

			if (not door_status) and (not exec_):
				self._safe_tag_write([exec_path], [True])
				self.log_event('Routing', reason='{}: door opened'.format(destination), ibn='', destination=destination, code=16)
				if bool(chute.get('occupied')):
					self.log_event(
						'Routing',
						reason='{}: items dropped on takeaway conveyor'.format(destination),
						ibn=self._dest_get(chute, 'ibns', ''),
						destination=destination,
						code=19
					)

			elif door_status and exec_:
				self._safe_tag_write([exec_path], [False])
				self.log_event('Routing', reason='{}: door closed'.format(destination), ibn='', destination=destination, code=17)
				if bool(self._dest_get(chute, 'waiting_for_processing', False)) and bool(chute.get('occupied')):
					self.log_event('Routing', reason='{}: waiting for processing'.format(destination), ibn='', destination=destination, code=18)
			else:
				self._safe_tag_write([exec_path], [False])

	def _check_processed_chutes_periodic(self):
		# FIX #3: each check uses its own timestamp so neither blocks the other.
		# Previously _last_check_processed_chutes was shared, causing
		# _check_key_updates_for_chutes to fire on every poll between 60-120s.
		now_ts = system.date.now()

		if system.date.millisBetween(self._last_check_key_updates, now_ts) >= 60000:
			self._check_key_updates_for_chutes()
			self._last_check_key_updates = now_ts

		if system.date.millisBetween(self._last_check_door_state, now_ts) >= 120000:
			self._check_door_state()
			self._last_check_door_state = now_ts

	def _check_key_updates_for_chutes(self):
		try:
			chutes = self.get_processing_status()
		except Exception:
			chutes = None

		if not chutes:
			return

		for chute in chutes:
			chute_id = chute.get('_id')
			if not chute_id:
				continue

			path = "[EuroSort]EuroSort/Level3/Destinations/{}/Destination/WCS_Processed".format(str(chute_id))
			self._safe_tag_write([path], [True])

			self.log_event('Routing', reason='{}: was processed to tote'.format(chute_id), destination=chute_id, code=20)
			self.log_event('Routing', reason='{}: was cleared and ready for new product'.format(chute_id), destination=chute_id, code=21)

	# -------------------------------------------------------------------------
	# Routing
	# -------------------------------------------------------------------------

	def _route_order(self, sorter_data):
		barcode = sorter_data.barcode
		issue = self._resolve_issue(barcode) or {}

		issue_id = str(issue.get('_id') or sorter_data.barcode or '')
		zone = str(issue.get('zone') or '')
		group_id = str(issue.get('group_id', '') or '')

		sorter_data.barcode = issue_id

		if issue_id.lower() in NOREAD_VALUES:
			self.scan_counts['scanner']['NoRead'] += 1
		else:
			self.scan_counts['scanner']['GoodRead'] += 1

		self.scan_counts['scanner']['TotalScans'] += 1
		total = float(self.scan_counts['scanner']['TotalScans'] or 0)
		good = float(self.scan_counts['scanner']['GoodRead'] or 0)
		self.scan_counts['scanner']['Rate'] = round((good / total) * 100.0, 2) if total else 0.0

		self._safe_tag_write('[EuroSort]EuroSort/Level3/Sorter_Control/Status/scan_counts', self.scan_counts)

		self.log_event('Routing', reason='Scanned {}'.format(issue_id), code=1, ibn=issue_id, destination='')
		self.log_event('Routing', reason='Looking for a chute matching Zone: {} and or Group_ID: {}'.format(zone, group_id), code=2, ibn='', destination='')

		destination = self._find_matching_chute(sorter_data, zone, group_id)

		if (not destination) and (zone not in ERROR_ZONES):
			self.log_event('Routing', reason='No chutes available for {}, getting next available.'.format(issue_id), code=5, ibn=issue_id, destination='')
			destination = self._get_next_available_chute(sorter_data, group_id, zone)
			if not destination:
				self.log_event('Routing', reason='No chutes available for {}'.format(issue_id), code=100, ibn=issue_id, destination='')

		return destination

	def _route_noread(self, sorter_data):
		if str(sorter_data.barcode or '').lower() not in NOREAD_VALUES:
			return None

		carrier_number = int(sorter_data.carrier_number)
		rec = self.carrier_get(carrier_number) or {}
		count = int(rec.get('recirculation_count', 0) or 0) + 1
		max_count = int(self.get_permissive('max_noread_recirc') or 0)

		self.carrier_update(carrier_number, {'recirculation_count': count})

		remaining = max_count - count
		try:
			self.logger.debug("NoRead recirc count is {}, will recirculate {} more times".format(count, remaining))
		except Exception:
			pass

		if max_count and count >= max_count:
			issue = (rec.get('issue_info') or {})
			ibn = issue.get('ibn') or issue.get('_id') or sorter_data.barcode
			self.log_event('Routing', reason='IBN: {} reached max recirc count routing to NoRead chute'.format(ibn), code=9, ibn=ibn, destination='')
			return self._find_matching_chute(sorter_data, 'NoRead', '-2')

		return None

	def _max_recirc(self, sorter_data):
		carrier_number = int(sorter_data.carrier_number)
		rec = self.carrier_get(carrier_number) or {}

		count = int(rec.get('recirculation_count', 0) or 0) + 1
		max_count = int(self.get_permissive('max_resort_recirc') or 0)

		self.carrier_update(carrier_number, {'recirculation_count': count})

		if max_count and count >= max_count:
			issue = (rec.get('issue_info') or {})
			ibn = issue.get('ibn') or issue.get('_id') or sorter_data.barcode
			self.log_event('Routing', reason='IBN: {} reached max recirc count forced to jackpot chute'.format(ibn), code=10, ibn=ibn, destination='')
			return self._find_matching_chute(sorter_data, 'Jackpot', '-3')

		return None

	def route_destination(self, sorter_data):
		try:
			for router in self._router_sequence:
				destination = router(sorter_data)
				if destination is not None:
					return destination
		except StopIteration as stop_looking:
			return stop_looking
		except Exception:
			return StopIteration

		return StopIteration

	# -------------------------------------------------------------------------
	# Error chute assignment
	# -------------------------------------------------------------------------

	def _assign_initial_error_chutes(self):
		lanes = ['lane1_enabled', 'lane2_enabled', 'lane3_enabled', 'lane4_enabled']

		for i, en in enumerate(lanes, start=1):
			if not bool(self.get_permissive(en)):
				continue

			lane_set = bool(self.get_permissive('lane{}_set'.format(i)))
			if lane_set:
				continue

			nr_tag = "{}/AutoAssign/Lane_{}/Max_Noread_Chutes".format(self.CONTROL_TAG_PATH, i)
			jp_tag = "{}/AutoAssign/Lane_{}/Max_Jackpot_Chutes".format(self.CONTROL_TAG_PATH, i)

			try:
				max_nr = int(system.tag.readBlocking([nr_tag])[0].value)
				max_jp = int(system.tag.readBlocking([jp_tag])[0].value)
			except Exception:
				max_nr, max_jp = 0, 0

			self._assign_error_chutes_for_lane(i, 'NoRead', max_nr)
			self._assign_error_chutes_for_lane(i, 'Jackpot', max_jp)

			self._safe_tag_write(['{}/AutoAssign/Lane_{}/Set'.format(self.CONTROL_TAG_PATH, i)], [True])

	def _assign_error_chutes_for_lane(self, lane, zone, count):
		filter_expr = {"$and": [
			{"lane": lane},
			{"zone": str(zone)},
			{"in_service": True},
		]}

		existing = self._find_destinations(filter_expr)
		needed = int(count or 0) - len(existing)

		self.log_event('Routing', reason='Lane {} changed from {} to {}'.format(lane, len(existing), count), code=27, ibn='', destination='')

		if needed <= 0:
			return

		filt = {"$and": [
			{"lane": lane},
			{"occupied": False},
			{"in_service": True},
			{"queued": False},
			{"faulted": False},
			{"wcs_processed": True},
		]}

		candidates = self._find_destinations(filt)
		if lane in (3, 4):
			candidates = candidates[::-1]

		for ch in candidates[:needed]:
			chute_id = ch.get('_id') or ch.get('destination')
			if not chute_id:
				continue

			group_id = "-2" if zone == "NoRead" else "-3"

			self.log_event(
				'Routing',
				reason='Assigned zone: {} and Group_Id: {} to chute:{}'.format(zone, group_id, chute_id),
				destination=chute_id,
				code=28,
				ibn=''
			)

			self._dest_update(
				chute_id,
				common_updates={
					'_id': chute_id,
					'occupied': True,
				},
				chute_updates={
					'zone': str(zone),
					'group_id': str(group_id),
					'wcs_processed': False,
				}
			)

	# -------------------------------------------------------------------------
	# Chute selection / assignment
	# -------------------------------------------------------------------------

	def _find_matching_chute(self, sorter_data, zone, group_id):
		if zone in ('NoRead', 'Jackpot'):
			group = 2
		else:
			group = int(self.get_permissive('group_by') or 0)

		group_query = {
			0: {'zone': str(zone)},
			1: {'group_id': str(group_id)},
			2: {'zone': str(zone), 'group_id': str(group_id)},
		}

		base_filt = {"$and": [
			group_query.get(group) or {},
			{"toteFull": False},
			{"occupied": True},
			{"in_service": True},
			{"queued": False},
			{"wcs_processed": False},
			{"waiting_for_processing": False},
			{"faulted": False},
		]}

		chutes = self._find_destinations(base_filt)
		if not chutes:
			return None

		for chute in chutes:
			destination = self._process_chute_result(chute, sorter_data, group_id, zone)
			if destination:
				return destination

		return None

	def _process_chute_result(self, chute_rec, sorter_data, group_id, zone):
		chute_id = chute_rec.get('_id') or chute_rec.get('destination')
		if not chute_id:
			return None

		carrier_number = int(sorter_data.carrier_number)

		cal_vol = self._calculate_volume_metrics_product(sorter_data.length, sorter_data.width, sorter_data.height)
		curr_volume = float(self._dest_get(chute_rec, 'volume', 0.0) or 0.0)
		new_volume, percent_full = self._calculate_volume_metrics(curr_volume, cal_vol)

		if new_volume >= float(self.get_permissive('tote_main_volume') or 0.0):
			return None

		issue_id = str(sorter_data.barcode or '')

		zones = self._parse_list_field(self._dest_get(chute_rec, 'zone', ''))
		ibns = self._parse_list_field(self._dest_get(chute_rec, 'ibns', ''))
		groups = self._parse_list_field(self._dest_get(chute_rec, 'group_id', ''))

		if zone and zone not in zones:
			zones.append(zone)
		if issue_id and issue_id not in ibns:
			ibns.append(issue_id)
		if group_id and str(group_id) not in groups:
			groups.append(str(group_id))

		tote_full = True if float(percent_full) >= float(self.get_permissive('max_fill') or 0.0) else False

		dest_updates_common = {
			'_id': chute_id,
			'occupied': True,
		}

		dest_updates_chute = {
			'volume': float(new_volume),
			'volume_percent_full': float(percent_full),
			'toteFull': bool(tote_full),
			'zone': ','.join(zones),
			'ibns': ','.join(ibns),
			'group_id': ','.join(groups),
			'wcs_processed': False,
			'waiting_for_processing': False,
		}

		issue_info = {
			'ibn': issue_id,
			'zone': zone,
			'group_id': str(group_id),
			'length': sorter_data.length,
			'width': sorter_data.width,
			'height': sorter_data.height,
			'volume': cal_vol,
			'assigned_dest': chute_id,
			'chuteName': chute_rec.get('chuteName') or self._dest_get(chute_rec, 'chute_name', '') or '',
		}

		self.assign_carrier_to_destination(
			carrier_number=carrier_number,
			dest_identifier=chute_id,
			scanner=getattr(sorter_data, 'scanner', None),
			transit_info=issue_info,
			assigned_name=issue_info.get('chuteName'),
			extra_carrier_updates={'recirculation_count': 1},
			extra_dest_updates=dict(dest_updates_common, chute_info=dest_updates_chute),
		)

		self.log_event('Routing', reason='Found chute for ibn: {}'.format(issue_id), code=3, ibn=issue_id, destination='')
		self.log_event('Routing', reason='Routing {} on carrier: {} to chute:{}'.format(issue_id, carrier_number, chute_id), ibn=issue_id, destination=chute_id, code=4)

		return chute_id

	def _get_next_available_chute(self, sorter_data, group_id, zone):
		sort_by = int(self.get_permissive('sort_by') or 0)

		try:
			last_used = str(system.tag.readBlocking(['[EuroSort]EuroSort/Level3/Control/Last_Used'])[0].value)
		except Exception:
			last_used = 'A'

		if sort_by == 1:
			for _ in range(2):
				search = {
					"A": {"$regex": "^C[0-9]{6}A$"},
					"B": {"$regex": "^C[0-9]{6}B$"},
				}
				side = 'B' if last_used == 'A' else 'A'
				filt = {"$and": [
					{"chuteName": search.get(side)},
					{"occupied": False},
					{"in_service": True},
					{"queued": False},
					{"faulted": False},
					{"wcs_processed": True},
				]}
				chutes = self._find_destinations(filt)
				if not chutes:
					last_used = side
					continue

				if int(chutes[0].get('lane', 0) or 0) in (3, 4):
					chutes = chutes[::-1]

				dest = self._process_chute_result(chutes[0], sorter_data, group_id, zone)
				if dest:
					self._safe_tag_write('[EuroSort]EuroSort/Level3/Control/Last_Used', side)
					return dest

				last_used = side

		elif sort_by == 2:
			lanes = [1, 2, 3, 4]
			num_lanes = len(lanes)

			start_idx = (lanes.index(self.last_sorted_lane) + 1) % num_lanes

			for _ in range(2):
				for i in range(num_lanes):
					next_lane = lanes[(start_idx + i) % num_lanes]
					if int(self.get_permissive("lane{}_enabled".format(next_lane)) or 0) != 1:
						continue

					filt = {"$and": [
						{"lane": next_lane},
						{"occupied": False},
						{"in_service": True},
						{"queued": False},
						{"faulted": False},
						{"wcs_processed": True},
					]}

					chutes = self._find_destinations(filt)
					if not chutes:
						continue

					if next_lane in (3, 4):
						chutes = chutes[::-1]

					self.last_sorted_lane = next_lane
					return self._process_chute_result(chutes[0], sorter_data, group_id, zone)

		return None

	# -------------------------------------------------------------------------
	# Verify / discharge handling
	# -------------------------------------------------------------------------

	def handle_verify(self, sorter_data):
		super(Level_3_OrderRouting, self).handle_verify(sorter_data)

		raw_dest = sorter_data.destination or ''
		if raw_dest in NODEST:
			return

		try:
			chute_fields = raw_dest.split('-')
			station = int(chute_fields[2])
			side = chute_fields[4]
			destination = 'DST-{station:04d}-1-1-{side}'.format(station=station, side=side)
		except Exception:
			try:
				destination = Destination.parse(raw_dest).destination
			except Exception:
				return

		chute_info = self.destination_get(destination) or {}
		carrier_number = int(sorter_data.carrier_number)

		carrier_rec = self.carrier_get(carrier_number) or {}
		issue_info = carrier_rec.get('issue_info') or {}

		if not isinstance(issue_info, dict) or not issue_info:
			issue = self._resolve_issue(sorter_data.barcode) or {}
			issue_info = {
				'ibn': str(issue.get('_id') or sorter_data.barcode or ''),
				'zone': str(issue.get('zone') or ''),
				'group_id': str(issue.get('group_id') or ''),
				'length': sorter_data.length,
				'width': sorter_data.width,
				'height': sorter_data.height,
				'volume': float(sorter_data.length or 0) * float(sorter_data.width or 0) * float(sorter_data.height or 0),
			}

		code = sorter_data.message_code

		if code == 18010:
			self.log_event('Routing', reason='Attempting to deliver ibn: {} to chute: {}'.format(issue_info.get('ibn'), destination), ibn=issue_info.get('ibn'), destination=destination, code=6)
			self.mark_carrier_attempted(carrier_number)
			return

		elif code == MessageCode.ITEM_DISCHARGED_AT_WRONG_DESTINATION:
			self.log_event('Routing', reason='Ibn:{} was delivered to {} from carrier:{}'.format(issue_info.get('ibn'), destination, carrier_number), ibn=issue_info.get('ibn'), destination=destination, code=42)
			self.divert_counts['diverts']['Failed_Wrong'] += 1
			self.mark_carrier_failed(carrier_number)

		elif code == MessageCode.DISCHARGE_ABORTED_POSITIONING_ERROR:
			self.log_event('Routing', reason='Ibn:{} was aborted due to positioning error on carrier:{}'.format(issue_info.get('ibn'), carrier_number), ibn=issue_info.get('ibn'), destination=destination, code=42)
			self.divert_counts['diverts']['Failed_Wrong'] += 1
			self.mark_carrier_aborted(carrier_number)

		elif code == MessageCode.DISCHARGE_ABORTED_DESTINATION_FULL:
			self.log_event('Routing', reason='{}: reached chute full sensor'.format(destination), ibn='', destination=destination, code=12)
			self.divert_counts['diverts']['Failed_Full'] += 1

			rec = self.carrier_get(carrier_number) or {}
			rc = int(rec.get('recirculation_count', 0) or 0) + 1
			self.carrier_update(carrier_number, {'recirculation_count': rc})

			self.log_event('Routing', reason='Failed to deliver ibn:{} to chute{}'.format(issue_info.get('ibn'), destination), ibn=issue_info.get('ibn'), destination=destination, code=7)
			self.mark_carrier_failed(carrier_number)

		elif code == MessageCode.DISCHARGED_AT_DESTINATION:
			zone = str(issue_info.get('zone') or '')
			if zone == 'NoRead':
				self.divert_counts['diverts']['Confirmed_NoRead'] += 1
			elif zone == 'Jackpot':
				self.divert_counts['diverts']['Confirmed_Jackpot'] += 1
			else:
				self.divert_counts['diverts']['Confirmed'] += 1

			self._finalize_discharge(destination, chute_info, issue_info, code)
			self.log_event('Routing', reason='Delivered ibn: {} to chute: {}'.format(issue_info.get('ibn'), destination), ibn=issue_info.get('ibn'), destination=destination, code=8)
			self.mark_carrier_delivered(carrier_number)

		else:
			if code not in [18010, 18011, 18013, 18005, 18026, 18004]:
				self.log_event('Routing', reason='ibn:{} to chute: {} from carrier:{} for code: {}'.format(issue_info.get('ibn'), destination, carrier_number, code), ibn=issue_info.get('ibn'), destination=destination, code=99)
				self.divert_counts['diverts']['Failed_Other'] += 1
				self.mark_carrier_unknown(carrier_number)

		total = (
			self.divert_counts['diverts']['Confirmed_NoRead']
			+ self.divert_counts['diverts']['Confirmed_Jackpot']
			+ self.divert_counts['diverts']['Confirmed']
			+ self.divert_counts['diverts']['Failed_Wrong']
		)
		self.divert_counts['diverts']['Total_Discharged'] = total
		self._safe_tag_write('[EuroSort]EuroSort/Level3/Sorter_Control/Status/divert_Counts', self.divert_counts)

	def _finalize_discharge(self, chute_id, chute_info, issue_info, message_code):
		chute_actual = chute_info or {}

		if not bool(chute_actual.get('occupied', False)):
			self.log_event('Routing', reason='{}: has been set to occupied'.format(chute_id), ibn='', destination=chute_id, code=29)

		original_vol = float(self._dest_get(chute_actual, 'volume', 0.0) or 0.0)
		extra_pct = float(self.get_permissive('extra_volume') or 0.0)

		issue_vol = float(issue_info.get('volume', 0.0) or 0.0)
		issue_vol = issue_vol + (issue_vol * extra_pct / 100.0)

		new_volume, percent_full = self._calculate_volume_metrics(original_vol, issue_vol)
		tote_full = True if float(percent_full) >= float(self.get_permissive('max_fill') or 0.0) else False

		if tote_full:
			self.log_event('Routing', reason='{}: reached tote full volume percentage'.format(chute_id), ibn='', destination=chute_id, code=11)

		ibns = self._parse_list_field(self._dest_get(chute_actual, 'ibns', ''))
		zones = self._parse_list_field(self._dest_get(chute_actual, 'zone', ''))
		groups = self._parse_list_field(self._dest_get(chute_actual, 'group_id', ''))

		ibn = str(issue_info.get('ibn') or '')
		zone = str(issue_info.get('zone') or '')
		gid = str(issue_info.get('group_id') or '')

		if ibn and ibn not in ibns:
			ibns.append(ibn)
		if zone and zone not in zones:
			zones.append(zone)
		if gid and gid not in groups:
			groups.append(gid)

		self._dest_update(
			chute_id,
			common_updates={
				'_id': chute_id,
				'occupied': True,
			},
			chute_updates={
				'volume': round(float(new_volume), 2),
				'volume_percent_full': round(float(percent_full), 2),
				'toteFull': bool(tote_full),
				'occupied': True,
				'zone': ','.join(zones),
				'group_id': ','.join(groups),
				'ibns': ','.join(ibns),
				'chuteCount': int(self._dest_get(chute_actual, 'chuteCount', 0) or 0) + 1,
				'wcs_processed': False,
				'waiting_for_processing': False,
			}
		)

		self._safe_tag_write([
			'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/Occupied'.format(chute_id),
			'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/WCS_Processed'.format(chute_id),
			'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/Waiting_For_Processing'.format(chute_id),
		], [True, False, False])

		if bool(self.get_permissive('auto_active')) and bool(tote_full) and (zone not in ['Jackpot', 'NoRead']):
			self._safe_tag_write([
				'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/Available'.format(chute_id),
				'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/Queued'.format(chute_id),
				'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/ToteFull'.format(chute_id),
			], [True, True, True])
			self.log_event('Routing', reason='{}: requested to be released'.format(chute_id), ibn='', destination=chute_id, code=13)

		elif bool(self.get_permissive('auto_active')) and bool(tote_full) and (zone in ['Jackpot', 'NoRead']):
			self._safe_tag_write(
				'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/ToteFull'.format(chute_id),
				True
			)

		elif (not bool(self.get_permissive('auto_active'))) and bool(tote_full):
			self._safe_tag_write(
				'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/ToteFull'.format(chute_id),
				True
			)

		if zone != 'NoRead':
			if ibn.lower() == 'noread':
				return

			if not bool(self.get_permissive('squelch_WCS')):
				self.notify_wcs_deliver(issue_info)
				self.log_event('Routing', reason='WCS notified ibn: {} delivered to chute {}'.format(ibn, chute_id), ibn=ibn, destination=chute_id, code=30)
			else:
				self.log_event('Routing', reason='WCS was not notified ibn: {} delivering to chute {}'.format(ibn, chute_id), ibn=ibn, destination=chute_id, code=30)


# ===========================================================================
# LEVEL 2
# ===========================================================================

class Level_2_OrderRouting(
	EuroSorterContentTracking,
	EuroSorterPermissivePolling,
	EuroSorterPolling,
	EuroSorterAccessWCS,
	EuroSorterLightControl,
):

	CONTROL_PERMISSIVE_TAG_MAPPING = {
		'max_noread_recirc': 'No Read recirc attempts',
		'squelch_wcs_updates': 'Squelch WCS',
		'max_resort_recirc': 'recirc attempts',
		'level3_dest': 'Level3_Dest',
		'clearance_height': '%s/clearance_height' % (get_dims),

		'by_max_h': '%s/height' % (get_max),
		'by_max_l': '%s/length' % (get_max),
		'by_max_w': '%s/width' % (get_max),
		'by_max_v': '%s/volume' % (get_max),
		'by_max_all': '%s/all' % (get_max),
		'by_max_any': '%s/any' % (get_max),

		'by_min_h': '%s/height' % (get_min),
		'by_min_l': '%s/length' % (get_min),
		'by_min_w': '%s/width' % (get_min),
		'by_min_v': '%s/volume' % (get_min),
		'by_min_all': '%s/all' % (get_min),
		'by_min_any': '%s/any' % (get_min),

		'max_dims': '%s/max_dims' % (get_dims),
		'min_dims': '%s/min_dims' % (get_dims),
		'tote_dims': '%s/tote_dims' % (get_dims),

		'ratio_long_short_ratio': '%s/ratio_long_short_ratio' % (get_max),
		'tube_ratio_flatness_ratio': '%s/tube_ratio_flatness_ratio' % (get_max),
		'box_ratio_flatness_ratio': '%s/box_ratio_flatness_ratio' % (get_max),
		'aspect_balance_ratio': '%s/aspect_balance_ratio' % (get_max),

		'reset_dict': 'clear_defaults',
		'reload_state': 'Reload Routes',
	}

	# Names that are never valid sort targets — skipped during vendor/zone matching
	# so a vendor or zone name can never accidentally land on an exception chute.
	SPECIAL_ASSIGNED_NAMES = frozenset([
		'LEVEL3', 'CROSSDOCK', 'JACKPOT', 'NOREAD',
		'UNRESOLVED', 'NOVENDOR', 'INSPECTION',
		'OVERSIZED-BAG', 'OVERSIZED-TUBE', 'OVERSIZED-BOX',
	])

	def __init__(self, name, **init_cfg):
		super(Level_2_OrderRouting, self).__init__(name, **init_cfg)
		self.logger = Logger(name)
		self.DEST_BASE_PATH = '[EuroSort]EuroSort/%s/Destinations' % name
		self.issue_info = {}
		self._last_check_processed_chutes = system.date.now()
		self.maxjackpot = 0
		self.maxnoread = 0

		self.scanner_id = None
		self.loaded_defaults = False

		self.DEST_STATUS_TAGS = {
			'in_service': 'In_Service',
			'dfs':        'DFS',
			'ofs':        'OFS',
			'faulted':    'Faulted',
			'status':     'Light/Status',
		}

		for perm, tag in self.CONTROL_PERMISSIVE_TAG_MAPPING.items():
			self._subscribe_control_permissive(perm, tag)

		self._polling_methods.append(self._refresh_destination_status_from_tags)

		self._init_polling()

		self.load_default_chutes()

		if self._gp('reset_dict', False):
			self.clear_all_destinations(reload_defaults=True)

	# ------------------------------------------------------------------
	# Helpers
	# ------------------------------------------------------------------

	def _gp(self, name, default=None):
		try:
			return self.get_permissive(name)
		except Exception:
			return default

	def load_default_chutes(self):
		if not self.loaded_defaults:
			chutes_to_load = {
				'Level3':    'DST-0120-1-1-A',
				'CrossDock': 'DST-0105-1-1-A',
			}
			for key, destination in chutes_to_load.items():
				chute = self.destination_get(destination) or {}

				current_names = self._dest_get(chute, 'assigned_name', []) or []
				if isinstance(current_names, basestring):
					current_names = [current_names]

				is_assigned = bool(self._dest_get(chute, 'assigned', False))

				if key not in current_names:
					current_names.append(key)
				if not is_assigned:
					is_assigned = True

				self._dest_update(
					destination,
					chute_updates={
						'assigned_name': current_names,
						'assigned':      is_assigned,
					}
				)
			self.loaded_defaults = True

	def clear_all_destinations(self, reload_defaults=True):
		updated = 0
		ts = system.date.now()

		try:
			self._initialize_destination_contents(full_clear=True)
			self.logger.info('Reinitialized destination contents cache for %s' % self.name)
		except Exception as e:
			self.logger.warn('Failed reinitializing destination contents cache: %s' % str(e))

		try:
			destination_names = list(self.destinations_all_transit_info().keys())
		except Exception as e:
			self.logger.error('Unable to get destinations for clear: %s' % str(e))
			destination_names = []

		for destination in destination_names:
			try:
				self._dest_update(
					destination,
					common_updates={
						'enroute':      0,
						'delivered':    0,
						'last_updated': ts,
					},
					chute_updates={
						'assigned_name': [],
						'assigned':      False,
						'assigned_mode': '',
						'transit_info':  {},
						'dfs':           False,
						'ofs':           False,
						'light_status':  'OFF',
					}
				)
				updated += 1
			except Exception as e:
				self.logger.warn('Failed clearing destination %s: %s' % (destination, str(e)))

		if reload_defaults:
			self.loaded_defaults = False
			self.load_default_chutes()

		return {'ok': True, 'data': {'updated': updated}, 'message': None}

	# ------------------------------------------------------------------
	# Tag status helpers
	# ------------------------------------------------------------------

	def _destination_status_tagpaths(self, dest_key):
		base   = self.DEST_BASE_PATH
		prefix = '%s/%s/Destination' % (base, dest_key)
		return {
			fn: '%s/%s' % (prefix, tag)
			for fn, tag in self.DEST_STATUS_TAGS.items()
		}

	def _encode_light_mode_to_tag(self, mode):
		mode = (mode or 'Off').upper()
		if mode == 'OFF':    return 0
		if mode == 'ON':     return 1
		if mode == 'BLINK1': return 2
		if mode == 'BLINK2': return 3
		return 0

	def _set_chute_light_mode(self, dest_key, mode):
		mode = (mode or 'Off').upper()
		if mode not in ('OFF', 'ON', 'BLINK1', 'BLINK2'):
			mode = 'OFF'

		self._dest_update(dest_key, chute_updates={'light_status': mode})

		try:
			tagpaths    = self._destination_status_tagpaths(dest_key)
			status_path = tagpaths.get('status')
			if status_path:
				system.tag.writeBlocking([status_path], [self._encode_light_mode_to_tag(mode)])
		except Exception:
			self.logger.warn('Failed to write light status tag for %s to mode %s' % (dest_key, mode))

	def _evaluate_assigned_group_lights(self, changed_dest_key):
		changed_rec   = self.destination_get(changed_dest_key) or {}
		assigned_name = self._dest_get(changed_rec, 'assigned_name')
		if not assigned_name:
			return

		def _has_assigned_name(rec, name):
			rec_name = self._dest_get(rec, 'assigned_name')
			if rec_name is None:
				return False
			if isinstance(rec_name, (list, tuple, set)):
				return name in rec_name
			return str(name) in str(rec_name)

		group_keys = []
		for dest_key in self._sorted_destinations():
			rec = self.destination_get(dest_key)
			if rec is None:
				continue
			if not _has_assigned_name(rec, assigned_name):
				continue
			if not rec.get('in_service', True):
				continue
			if self._dest_get(rec, 'dfs', False) or self._dest_get(rec, 'ofs', False) or rec.get('faulted', False):
				continue
			group_keys.append(dest_key)

		if not group_keys:
			return

		full_map  = {dk: bool(self._dest_get(self.destination_get(dk) or {}, 'dfs', False)) for dk in group_keys}
		total     = len(group_keys)
		num_full  = sum(1 for v in full_map.values() if v)

		if num_full == 0:
			for dk in group_keys:
				self._set_chute_light_mode(dk, 'OFF')
		elif 0 < num_full < total:
			for dk, is_full in full_map.items():
				self._set_chute_light_mode(dk, 'ON' if is_full else 'OFF')
		else:
			for dk in group_keys:
				self._set_chute_light_mode(dk, 'BLINK1')

	def _on_destination_status_changed(self, dest_key, changed):
		if any(k in changed for k in ('dfs', 'in_service', 'ofs', 'faulted')):
			self._evaluate_assigned_group_lights(dest_key)

	def _refresh_destination_status_from_tags(self):
		try:
			all_dest = list(self.destinations_all_transit_info().keys())
		except Exception:
			return
		if not all_dest:
			return

		read_paths, meta = [], []
		for dest_key in all_dest:
			for field_name, path in self._destination_status_tagpaths(dest_key).items():
				if field_name == 'status':
					continue
				read_paths.append(path)
				meta.append((dest_key, field_name))

		if not read_paths:
			return

		results         = system.tag.readBlocking(read_paths)
		updates_by_dest = {}
		for (dest_key, field_name), r in zip(meta, results):
			try:
				q = getattr(r, 'quality', None)
				if q is not None and not q.isGood():
					continue
				updates_by_dest.setdefault(dest_key, {})[field_name] = bool(r.value)
			except Exception:
				continue

		for dest_key, updates in updates_by_dest.items():
			current        = self.destination_get(dest_key) or {}
			changed_common = {}
			changed_chute  = {}
			for k, v in updates.items():
				if k in ('dfs', 'ofs'):
					if self._dest_get(current, k) != v:
						changed_chute[k] = v
				else:
					if current.get(k) != v:
						changed_common[k] = v
			if changed_common or changed_chute:
				self._dest_update(dest_key, changed_common, changed_chute)
				merged = dict(changed_common, **changed_chute)
				self._on_destination_status_changed(dest_key, merged)

	def _update_destination_status(self, dest_key, **fields):
		if not fields:
			return

		common_updates, chute_updates = {}, {}
		for field_name, value in fields.items():
			if field_name in ('dfs', 'ofs'):
				chute_updates[field_name] = value
			else:
				common_updates[field_name] = value

		self._dest_update(dest_key, common_updates, chute_updates)

		tagpaths     = self._destination_status_tagpaths(dest_key)
		write_paths  = []
		write_values = []
		for field_name, value in fields.items():
			if field_name not in tagpaths or field_name == 'status':
				continue
			write_paths.append(tagpaths[field_name])
			write_values.append(bool(value))

		if write_paths:
			try:
				system.tag.writeBlocking(write_paths, write_values)
			except Exception:
				self.logger.warn('Failed to write status tags for %s: %r' % (dest_key, fields))

		self._on_destination_status_changed(dest_key, fields)

	# ------------------------------------------------------------------
	# Recirculation helpers
	# ------------------------------------------------------------------

	def _route_noread(self):
		"""
		Checks if this NOREAD item has hit the max_noread_recirc ceiling.
		Recirculation count is incremented by handle_verify on DISCHARGE_ABORTED_*
		so we only read it here — never write it.
		"""
		max_count    = int(self._gp('max_noread_recirc', 0) or 0)
		carrier      = self.carrier or {}
		recirc_count = int(carrier.get('recirculation_count', 0) or 0)
		carrier_num  = carrier.get('carrier_number', None)

		self.logger.info(
			'NOREAD recirc check carrier=%s recirc=%s max=%s'
			% (carrier_num, recirc_count, max_count)
		)

		if max_count > 0 and recirc_count >= max_count:
			return self._find_chute_by_name('NOREAD', allow_special=True)

		return None

	def _max_recirc(self):
		"""
		Checks if this item has hit the hard max_resort_recirc ceiling.
		Recirculation count is incremented by handle_verify on DISCHARGE_ABORTED_*
		so we only read it here — never write it.
		"""
		max_recirc   = int(self._gp('max_resort_recirc', 0) or 0)
		carrier      = self.carrier or {}
		recirc_count = int(carrier.get('recirculation_count', 0) or 0)
		carrier_num  = carrier.get('carrier_number', None)

		self.logger.info(
			'MAX_RECIRC check carrier=%s recirc=%s max=%s'
			% (carrier_num, recirc_count, max_recirc)
		)

		if max_recirc > 0 and recirc_count >= max_recirc:
			return self._find_chute_by_name('JACKPOT', allow_special=True)

		return None

	# ------------------------------------------------------------------
	# Core chute finder
	# ------------------------------------------------------------------

	def _is_special_destination(self, dest_key, rec=None):
		"""
		Returns True if this chute is reserved for exception/special routing
		and must not be matched during normal vendor/zone sort matching.
		"""
		rec   = rec or {}
		names = self._dest_get(rec, 'assigned_name', []) or []
		if isinstance(names, basestring):
			names = [names]
		names = set(str(x).strip().upper() for x in names if x)

		if dest_key in ('DST-0120-1-1-A', 'DST-0105-1-1-A'):
			return True

		return bool(names & self.SPECIAL_ASSIGNED_NAMES)

	def _assigned_name_matches(self, target_name, assigned_value):
		def _canon(v):
			try:
				return str(v).strip().upper() if v is not None else ''
			except Exception:
				return ''

		def _as_list(v):
			if v is None:
				return []
			if isinstance(v, basestring):
				s = _canon(v)
				return [s] if s else []
			if isinstance(v, (list, tuple, set)):
				return [_canon(x) for x in v if x is not None and _canon(x)]
			return [_canon(v)] if _canon(v) else []

		target_name = _canon(target_name)
		if not target_name:
			return False

		for token in _as_list(assigned_value):
			if token == target_name:
				return True
			# Range token e.g. [A-C]
			m = re.match(r'^\[([A-Z])-([A-Z])\]$', token)
			if m and len(target_name) == 1:
				if m.group(1) <= target_name <= m.group(2):
					return True

		return False

	def _find_chute_by_name(self, target_name, pattern_re=None, building_id=None, allow_special=False):
		"""
		Single unified chute finder used by all routing paths.

		Args:
		    target_name:   assigned_name to match (vendor, zone, JACKPOT, NOREAD, etc.)
		    pattern_re:    compiled regex to filter by WCS chute name (B####(1|2)1A/B)
		    building_id:   if provided, only chutes with matching building_id qualify
		    allow_special: True when intentionally looking for an exception chute
		                   (JACKPOT, NOREAD, INSPECTION, OVERSIZED-*, etc.)

		Returns:
		    dest_key string or None
		"""
		def _canon(v):
			try:
				return str(v).strip().upper() if v is not None else ''
			except Exception:
				return ''

		target_name = _canon(target_name)
		if not target_name:
			return None

		for dest_key in self._sorted_destinations():
			rec = self.destination_get(dest_key)
			if rec is None:
				continue

			# FIX #4 — use 'chuteName' (top-level, set by contents.py) not 'chute_name'
			chute_code = rec.get('chuteName') or dest_key
			if pattern_re and chute_code and not pattern_re.match(chute_code):
				continue

			if not rec.get('in_service', True):
				continue

			if self._dest_get(rec, 'dfs', False) or self._dest_get(rec, 'ofs', False) or rec.get('faulted', False):
				continue

			if not bool(self._dest_get(rec, 'assigned', False)):
				continue

			if building_id and self._dest_get(rec, 'building_id') != building_id:
				continue

			# FIX #5 — consistent special-destination guard using SPECIAL_ASSIGNED_NAMES
			if not allow_special:
				if self._is_special_destination(dest_key, rec):
					continue

			if not self._assigned_name_matches(target_name, self._dest_get(rec, 'assigned_name')):
				continue

			return dest_key

		return None

	# ------------------------------------------------------------------
	# Routing — full priority sequence
	# ------------------------------------------------------------------

	def _route_order(self, assigned_name, assigned_mode):
		"""
		FIX #7 — single entry point that enforces the correct priority order:

		  1. Tote / clearance_over    -> upper-only chute (pattern 1)
		  2. Undersized               -> Level3 dest (off-sorter)
		  3. building_id == 1         -> CrossDock
		  4. Inspection flag          -> INSPECTION chute (pattern 3)
		  5. Oversized                -> Oversized-{shape} chute (pattern 1)
		  6. PRE + no vendor_name     -> NOVENDOR chute
		  7. PRE vendor exact match   -> vendor chute (pattern 3)
		  8. PRE first-letter match   -> letter-range chute (pattern 3)
		  9. POST zone match          -> zone chute (pattern 3, building_id filtered)
		 10. Fallback                 -> JACKPOT
		"""
		issue_info    = self.issue_info or {}
		is_tote       = bool(issue_info.get('is_tote', False))
		clearance_over = bool(issue_info.get('clearance_over', False))
		missing_dims  = self._has_missing_dims(issue_info)
		oversized     = bool(issue_info.get('oversized', False))
		undersized    = bool(issue_info.get('undersized', False))
		inspect       = bool(issue_info.get('inspect', False))
		building_id   = issue_info.get('building_id')
		vendor_name   = issue_info.get('vendor_name')
		shape         = str(issue_info.get('shape', 'bag')).lower()

		def _canon(v):
			try:
				return str(v).strip().upper() if v is not None else ''
			except Exception:
				return ''

		assigned_mode = _canon(assigned_mode)

		# Chute name regex patterns
		# Pattern 1: upper only  (B####21A/B) — totes, oversized, clearance
		# Pattern 3: any level   (B####(1|2)1A/B) — normal sort
		re_upper = re.compile(r'^B[0-9]{4}21(A|B)$')
		re_any   = re.compile(r'^B[0-9]{4}(1|2)1(A|B)$')

		# ── 1. Tote / clearance_over ──────────────────────────────────
		# Must go upper-only. For POST with missing dims, same rule applies.
		if is_tote or clearance_over or (missing_dims and assigned_mode != 'POST'):
			dest = self._find_chute_by_name(assigned_name, pattern_re=re_upper)
			if dest:
				return dest

		# ── 2. Undersized ─────────────────────────────────────────────
		# Route off-sorter to Level3. Inspection overrides undersized
		# (inspected items must be physically checked first).
		if undersized and not inspect:
			level3_dest = self._gp('level3_dest')
			if level3_dest:
				return level3_dest

		# ── 3. Building 1 -> CrossDock ────────────────────────────────
		if building_id == 1:
			dest = self._find_chute_by_name('CROSSDOCK', allow_special=True)
			if dest:
				return dest

		# ── 4. Inspection ─────────────────────────────────────────────
		# inspect flag from WCS collection + inspection chutes configured
		if inspect:
			dest = self._find_chute_by_name('INSPECTION', pattern_re=re_any, allow_special=True)
			if dest:
				return dest
			# No inspection chute available — fall through to normal routing

		# ── 5. Oversized ──────────────────────────────────────────────
		# Oversized-bag / Oversized-tube / Oversized-box, upper only
		if oversized:
			size_mode = 'OVERSIZED-%s' % shape.upper()
			dest = self._find_chute_by_name(size_mode, pattern_re=re_upper, allow_special=True)
			if dest:
				return dest
			# No oversized chute configured — fall through to normal routing

		# ── 6-8. PRE (vendor sort) ────────────────────────────────────
		if assigned_mode == 'PRE':
			# 6. No vendor_name -> NOVENDOR
			if vendor_name is None:
				self.logger.info(
					'PRE no vendor_name for assigned_name=%s — routing to NOVENDOR' % assigned_name
				)
				dest = self._find_chute_by_name('NOVENDOR', pattern_re=re_any, allow_special=True)
				if dest:
					return dest
				self.logger.warn('NOVENDOR chute not found, falling through to PRE letter routing')

			# 7. Exact vendor name match
			dest = self._find_chute_by_name(assigned_name, pattern_re=re_any)
			if dest:
				return dest

			# 8. First-letter fallback (e.g. chute assigned [A-C])
			first_letter = assigned_name[:1] if assigned_name else ''
			if first_letter:
				dest = self._find_chute_by_name(first_letter, pattern_re=re_any)
				if dest:
					return dest

		# ── 9. POST (zone sort) ───────────────────────────────────────
		elif assigned_mode == 'POST':
			# POST with missing dims -> Level3
			if missing_dims:
				level3_dest = self._gp('level3_dest')
				if level3_dest:
					return level3_dest

			dest = self._find_chute_by_name(
				assigned_name,
				pattern_re=re_any,
				building_id=building_id,
			)
			if dest:
				return dest

		# ── 10. JACKPOT fallback ──────────────────────────────────────
		self.logger.info(
			'_route_order fallback to JACKPOT for assigned_name=%s assigned_mode=%s'
			% (assigned_name, assigned_mode)
		)
		return self._find_chute_by_name('JACKPOT', allow_special=True)

	# ------------------------------------------------------------------
	# Carrier helpers
	# ------------------------------------------------------------------

	def _validate_destination(self, destination):
		chute = self.destination_get(destination)
		if chute is None:
			return None
		if not chute.get('in_service', True):
			return None
		if self._dest_get(chute, 'dfs', False) or self._dest_get(chute, 'ofs', False) or chute.get('faulted', False):
			return None
		return destination

	def _get_carrierinfo(self, carrier_num):
		self.carrier = self.carrier_get(carrier_num)
		if self.carrier is None:
			return None

		destination = self.carrier.get('destination')
		if not destination:
			return None

		self.issue_info = self.carrier.get('issue_info', {}) or {}
		self.logger.info('%s: %s' % (carrier_num, destination))

		if self.issue_info:
			recirc_count = self.carrier.get('recirculation_count', 0)
			if self.scanner_id == self.carrier.get('induct_scanner', None):
				recirc_count += 1

			if self.carrier.get('discharged_attempted', False):
				self.carrier_update(carrier_num, discharged_attempted=False, destination=None)

			if destination:
				destination = self._validate_destination(destination)

			return destination

		return None

	def get_carrier_update_info(self, carrier_number):
		rec = self.carrier_get(carrier_number)
		if rec is None:
			return None, None
		dest     = rec.get('destination', None)
		track_id = rec.get('track_id', None)
		if dest:
			dest = Destination.parse(dest)
		return dest, track_id or None

	def get_carrier_destination(self, carrier_number):
		rec = self.carrier_get(carrier_number)
		if rec is None:
			return None
		return rec.get('destination', None) or None

	def get_carrier_issue(self, carrier_number):
		rec = self.carrier_get(carrier_number)
		if rec is None:
			return None
		return rec.get('issue_info', None) or {}

	# ------------------------------------------------------------------
	# Main routing entry point
	# ------------------------------------------------------------------

	def route_destination(self, sorter_data):
		"""
		Priority order:
		  1.  Missing carrier number         -> error, return None
		  2.  NOREAD barcode                 -> _route_noread (recirc or NOREAD chute)
		  3.  DST barcode                    -> direct destination
		  4.  Existing valid assignment      -> return it
		  5.  WCS lookup / barcode detection -> define_and_detect
		  6.  Dims + size determination
		  7.  Max recirc ceiling             -> JACKPOT if exceeded
		  8.  Exception routers (NOCODE/NOSCAN/JACKPOT/UNRESOLVED/SDR)
		  9.  _route_order (full priority sequence — see docstring)
		 10.  Final JACKPOT fallback
		"""
		carrier_num = sorter_data.carrier_number
		track_id    = sorter_data.track_id

		self.scanner_id = sorter_data.station_id

		# ── 1. Guard ──────────────────────────────────────────────────
		if carrier_num in (None, '', 'None'):
			self.logger.error(
				'route_destination missing carrier_number; station_id=%r track_id=%r barcodes=%r'
				% (
					getattr(sorter_data, 'station_id', None),
					getattr(sorter_data, 'track_id',   None),
					getattr(sorter_data, 'barcodes',   None),
				)
			)
			return None

		self.carrier = self.carrier_get(carrier_num) or {}

		self.carrier_update(carrier_num, induct_scanner=self.scanner_id, track_id=track_id)

		system.util.getLogger('dims').info('dims are :' + str(sorter_data.dimensions))

		prev_dest          = self.carrier.get('destination', None)
		prev_assigned_name = self.carrier.get('assigned_name', None)
		prev_assigned_mode = self.carrier.get('assigned_mode', None)

		self.issue_info = self.carrier.get('issue_info', {}) or {}
		self.issue_info.update({'induct_scanner': self.scanner_id})

		# ── 2-4. Detect barcodes ──────────────────────────────────────
		code, assigned_name, assigned_mode, router = self.define_and_detect(sorter_data.barcodes)

		# ── 2. NOREAD ─────────────────────────────────────────────────
		if router == 'NOREAD':
			dest = self._route_noread()
			if dest:
				self._assign_and_return(carrier_num, dest, track_id, assigned_name, assigned_mode)
				return dest
			# Under max recirc — item recirculates, no assignment yet
			return None

		# ── 3. Direct destination barcode ─────────────────────────────
		if router == 'DST':
			dest = self._validate_destination(code)
			if dest:
				self._assign_and_return(carrier_num, dest, track_id, assigned_name, assigned_mode)
				return dest

		# ── 4. Re-use existing valid assignment ───────────────────────
		if prev_dest and prev_assigned_name:
			if self._validate_destination(prev_dest):
				return prev_dest
			# Prev dest gone — try to re-route to same label
			dest = self._route_order(prev_assigned_name, prev_assigned_mode)
			if dest:
				self._assign_and_return(carrier_num, dest, track_id, prev_assigned_name, prev_assigned_mode)
				return dest

		# ── 5-6. Populate issue_info with dims and size ───────────────
		detected_is_tote = bool((self.issue_info or {}).get('is_tote', False))
		induct_scanner   = (self.issue_info or {}).get('induct_scanner')

		self.issue_info = {
			'barcode':        code,
			'assigned_name':  assigned_name,
			'assigned_mode':  assigned_mode,
			'router':         router,
			'is_tote':        detected_is_tote,
			'induct_scanner': induct_scanner,
		}

		self.calculate_product_dims(sorter_data.length, sorter_data.width, sorter_data.height)

		if not self.issue_info.get('is_tote', False):
			self.determine_product_size()

		self.carrier_update(
			carrier_num,
			issue_info    = self.issue_info,
			assigned_name = assigned_name,
			assigned_mode = assigned_mode,
		)

		destination = None

		try:
			# ── 7. Hard recirc ceiling ────────────────────────────────
			destination = self._max_recirc()

			# ── 8. Exception routers ──────────────────────────────────
			if destination is None:
				if router in ('NOCODE', 'NOSCAN', 'JACKPOT', 'UNRESOLVED', 'SDR'):
					# These all carry an assigned_name set to the router label
					# (e.g. 'JACKPOT', 'UNRESOLVED') — look up the matching chute.
					destination = self._find_chute_by_name(assigned_name, allow_special=True)

			# ── 9. Full priority route ────────────────────────────────
			if destination is None:
				destination = self._route_order(assigned_name, assigned_mode)

			# ── Assign and return ─────────────────────────────────────
			if destination is not None:
				self._assign_and_return(carrier_num, destination, track_id, assigned_name, assigned_mode)
				self.logger.info(
					'route_destination selected destination=%s carrier=%s router=%s recirc=%s'
					% (
						destination, carrier_num, router,
						(self.carrier_get(carrier_num) or {}).get('recirculation_count', 0)
					)
				)
				return destination

			# ── 10. Final JACKPOT fallback ────────────────────────────
			destination = self._find_chute_by_name('JACKPOT', allow_special=True)
			if destination:
				self._assign_and_return(carrier_num, destination, track_id, 'JACKPOT', assigned_mode)
				self.logger.info(
					'route_destination final JACKPOT fallback carrier=%s router=%s' % (carrier_num, router)
				)
				return destination

		except Exception:
			self.logger.warn(
				'Level_2_OrderRouting.route_destination error: %s' % python_full_stack()
			)
			return None

		return None

	def _assign_and_return(self, carrier_num, destination, track_id, assigned_name, assigned_mode):
		"""Assigns carrier to destination and logs. Extracted to avoid repetition."""
		self.assign_carrier_to_destination(
			carrier_num,
			destination,
			track_id      = track_id,
			scanner       = self.scanner_id,
			assigned_name = assigned_name,
			assigned_mode = assigned_mode,
			transit_info  = self.issue_info,
		)

	# ------------------------------------------------------------------
	# Size / dims helpers
	# ------------------------------------------------------------------

	def _has_missing_dims(self, issue_info=None):
		issue_info = issue_info or self.issue_info or {}
		try:
			l = _to_float(issue_info.get('length'))
			w = _to_float(issue_info.get('width'))
			h = _to_float(issue_info.get('height'))
		except Exception:
			return True
		return l <= 0 or w <= 0 or h <= 0

	def calculate_product_dims(self, l, w, h):
		tote_dims = self._gp('tote_dims', {}) or {}
		is_tote   = self.issue_info.get('is_tote', False)

		if is_tote:
			l = round(_to_float(tote_dims.get('length')), 2)
			w = round(_to_float(tote_dims.get('width')),  2)
			h = round(_to_float(tote_dims.get('height')), 2)
			self.issue_info.update({
				'size_reason':    ['Found Tote'],
				'shape':          'Tote',
				'clearance_over': True,
			})
		else:
			l = round(_to_float(l), 2)
			w = round(_to_float(w), 2)
			h = round(_to_float(h), 2)

		self.issue_info.update({
			'length': l,
			'width':  w,
			'height': h,
			'volume': round(_volume(l, w, h), 2),
		})

	def define_and_detect(self, barcodes):
		dst  = None
		tote = None
		sdr  = None
		ibns = []

		error_seen = {'NOREAD': False, 'NOSCAN': False, 'NOCODE': False}

		for raw in barcodes:
			code = raw
			if not code:
				continue

			for patt, err_label in error_matches.items():
				if patt.match(code):
					if err_label in error_seen:
						error_seen[err_label] = True
					break
			else:
				for patt, label in code_matches.items():
					if patt.match(code):
						if label == 'DST':
							dst = code
						elif label == 'TOTE' and tote is None:
							tote = code
							ibns = None
							self.issue_info['is_tote'] = True
						elif label == 'SDR' and sdr is None:
							sdr = code
						elif label == 'IBN':
							ibns.append(code)
						break

		if dst:
			self.issue_info['codes'] = dst
			return dst, 'DST', 'Destination', 'DST'

		if sdr:
			self.issue_info['codes'] = sdr
			return sdr, 'SDR', 'SDR', 'SDR'

		lookup_codes = []
		if tote:
			lookup_codes.append(tote)
		if ibns and not tote:
			lookup_codes.extend(ibns)

		if lookup_codes:
			first_code, assigned_name, assigned_mode, router = self.wcs_lookup(lookup_codes)

			if self.issue_info.get('inspect', False):
				self.issue_info['size_mode'] = '%s-inspection' % self.issue_info.get('shape', 'bag')

			return first_code, assigned_name, assigned_mode, router

		if error_seen['NOREAD']:
			return 'NOREAD',     'NOREAD',     'NOREAD',     'NOREAD'
		if error_seen['NOCODE']:
			return 'NOCODE',     'NOCODE',     'NOCODE',     'NOCODE'
		if error_seen['NOSCAN']:
			return 'NOSCAN',     'NOSCAN',     'NOSCAN',     'NOSCAN'

		return     'UNRESOLVED', 'UNRESOLVED', 'UNRESOLVED', 'UNRESOLVED'

	def determine_product_size(self):
		size_reason = []
		oversized   = False
		undersized  = False

		max_dims = self._gp('max_dims', {}) or {}
		min_dims = self._gp('min_dims', {}) or {}

		flags = {
			'min_l':   bool(self._gp('by_min_l',   False)),
			'min_w':   bool(self._gp('by_min_w',   False)),
			'min_h':   bool(self._gp('by_min_h',   False)),
			'min_v':   bool(self._gp('by_min_v',   False)),
			'min_any': bool(self._gp('by_min_any', False)),
			'min_all': bool(self._gp('by_min_all', False)),
			'max_l':   bool(self._gp('by_max_l',   False)),
			'max_w':   bool(self._gp('by_max_w',   False)),
			'max_h':   bool(self._gp('by_max_h',   False)),
			'max_v':   bool(self._gp('by_max_v',   False)),
			'max_any': bool(self._gp('by_max_any', False)),
			'max_all': bool(self._gp('by_max_all', False)),
		}

		thr = {
			'min_l': _to_float(min_dims.get('length', 0.0)),
			'min_w': _to_float(min_dims.get('width',  0.0)),
			'min_h': _to_float(min_dims.get('height', 0.0)),
			'min_v': _to_float(min_dims.get('volume', 0.0)),
			'max_l': _to_float(max_dims.get('length', 0.0)),
			'max_w': _to_float(max_dims.get('width',  0.0)),
			'max_h': _to_float(max_dims.get('height', 0.0)),
			'max_v': _to_float(max_dims.get('volume', 0.0)),
			'clr_h': _to_float(self._gp('clearance_height', 12.0)),
		}

		l = _to_float(self.issue_info.get('length'))
		w = _to_float(self.issue_info.get('width'))
		h = _to_float(self.issue_info.get('height'))
		v = _to_float(self.issue_info.get('volume'))

		min_checks = []
		if flags['min_l']: min_checks.append(('length<min_length', l < thr['min_l']))
		if flags['min_w']: min_checks.append(('width<min_width',   w < thr['min_w']))
		if flags['min_h']: min_checks.append(('height<min_height', h < thr['min_h']))
		if flags['min_v']: min_checks.append(('volume<min_volume', v < thr['min_v']))

		max_checks = []
		if flags['max_l']: max_checks.append(('length>max_length', l > thr['max_l']))
		if flags['max_w']: max_checks.append(('width>max_width',   w > thr['max_w']))
		if flags['max_h']: max_checks.append(('height>max_height', h > thr['max_h']))
		if flags['max_v']: max_checks.append(('volume>max_volume', v > thr['max_v']))

		if flags['min_all']:
			if min_checks and all(r for _, r in min_checks):
				size_reason.append('Undersized'); undersized = True
		elif flags['min_any']:
			if any(r for _, r in min_checks):
				size_reason.append('Undersized'); undersized = True
		else:
			for reason, result in min_checks:
				if result:
					size_reason.append(reason); undersized = True

		if flags['max_all']:
			if max_checks and all(r for _, r in max_checks):
				size_reason.append('Oversized'); oversized = True
		elif flags['max_any']:
			if any(r for _, r in max_checks):
				size_reason.append('Oversized'); oversized = True
		else:
			for reason, result in max_checks:
				if result:
					size_reason.append(reason); oversized = True

		clearance_over = (h > thr['clr_h'])

		dims    = [l, w, h]
		longest = max(dims)
		shortest = min(dims) if longest > 0 else 0.0
		mid     = sorted(dims)[1]

		ratio_long_short = (longest / shortest) if shortest > 0 else 0.0
		ratio_flatness   = (h / mid)             if mid > 0     else 0.0

		rls_thresh       = _to_float(self._gp('ratio_long_short_ratio',    3.0))
		tube_flat_thresh = _to_float(self._gp('tube_ratio_flatness_ratio', 0.5))
		box_flat_thresh  = _to_float(self._gp('box_ratio_flatness_ratio',  0.8))

		if ratio_long_short >= rls_thresh and ratio_flatness < tube_flat_thresh:
			shape = 'tube'
		elif ratio_flatness >= box_flat_thresh:
			shape = 'box'
		else:
			shape = 'bag'

		size_mode = ''
		if oversized:  size_mode = 'Oversized-%s' % shape
		if undersized: size_mode = 'Undersized-%s' % shape

		self.issue_info.update({
			'length':         l,
			'width':          w,
			'height':         h,
			'volume':         v,
			'undersized':     undersized,
			'oversized':      oversized,
			'size_reason':    size_reason,
			'shape':          shape,
			'clearance_over': clearance_over,
			'size_mode':      size_mode,
		})

	# ------------------------------------------------------------------
	# Verify handling
	# ------------------------------------------------------------------

	def handle_verify(self, sorter_data):
		"""
		Discharge event handler. This is the ONLY place recirculation_count
		is incremented — on DISCHARGE_ABORTED_* events, meaning the item
		physically attempted a chute and was turned away. _route_noread and
		_max_recirc only read the count; they never write it.
		"""
		super(Level_2_OrderRouting, self).handle_verify(sorter_data)

		raw_dest = sorter_data.destination
		if not raw_dest:
			return

		chute_fields = raw_dest.split('-')
		if len(chute_fields) < 5:
			return

		destination = 'DST-{station:04d}-{chute}-1-{side}'.format(
			station = int(chute_fields[2]),
			chute   = chute_fields[3],
			side    = chute_fields[4],
		)

		carrier_num          = sorter_data.carrier_number
		self.issue_info      = self.get_carrier_issue(carrier_num) or {}
		message              = sorter_data.message_code
		rec                  = self.carrier_get(carrier_num)
		discharged_attempted = rec.get('discharged_attempted', False) if rec else False

		self.logger.info('%s:%s' % (carrier_num, self.issue_info))
		self.logger.info('%s:%s....type:%s' % (sorter_data.message_code, message, type(message)))

		if message == MessageCode.DISCHARGE_ATTEMPTED:
			if not discharged_attempted:
				self.mark_carrier_attempted(carrier_num)

		elif message == MessageCode.DISCHARGED_AT_DESTINATION:
			self.mark_carrier_delivered(carrier_num)

		elif message == MessageCode.DISCHARGE_FAILED:
			self.mark_carrier_failed(carrier_num)

		elif message in (
			MessageCode.DISCHARGE_ABORTED_DESTINATION_FULL,
			MessageCode.DISCHARGE_ABORTED_POSITIONING_ERROR,
		):
			# Increment here — item physically recirculated past a full or
			# mispositioned chute. _route_noread / _max_recirc read this value
			# to decide when the ceiling has been reached.
			recirc = int((rec.get('recirculation_count', 0) if rec else 0) or 0) + 1
			self.carrier_update(carrier_num, recirculation_count=recirc)
			self.logger.info(
				'handle_verify ABORTED carrier=%s recirc=%s' % (carrier_num, recirc)
			)
			self.mark_carrier_aborted(carrier_num)

		else:
			self.mark_carrier_unknown(carrier_num)

	def _process_chute_result(self, chute_name):
		rec = self.destination_get(chute_name)
		if rec is None:
			return chute_name

		transit_info = self._dest_get(rec, 'transit_info', {}) or {}
		transit_info['lastUpdated'] = datetime.now()
		self._dest_update(chute_name, chute_updates={'transit_info': transit_info})
		return chute_name

## ===========================================================================
# LEVEL 3 SHIP
# ===========================================================================


# ---------------------------------------------------------------------------
# Permissive tag path prefixes — relative to:
#   [EuroSort]EuroSort/Level3_Ship/Control/
# ---------------------------------------------------------------------------
_OB      = 'OB/'
_PACKOUT = 'Packout/'
_PURGE   = 'Purge/'
_AGING   = 'OrderAging/'
_DIMS    = 'Dims/'

# ---------------------------------------------------------------------------
# Chute types that accept consolidation items
# ---------------------------------------------------------------------------
CONSOLIDATION_CHUTE_TYPES = frozenset(['NORMAL', 'HP'])

# ---------------------------------------------------------------------------
# Carrier flipper lockout
#
# Physical constraint: after a carrier is diverted to a station, the flipper
# needs 4 carriers to pass before it can switch to a DIFFERENT position at
# the same station (different chute or different side).
#
# Same station + SAME position (chute+side)      -> always OK, flipper set
# Same station + DIFFERENT position (chute|side) -> gap must be >= 4
# ---------------------------------------------------------------------------
CARRIER_FLIPPER_LOCKOUT = 4

# Order statuses that are no longer active
INVALID_ORDER_STATUSES = frozenset(['cancelled', 'shipped'])

# ---------------------------------------------------------------------------
# Induction point -> nearest downstream packout station
#
# Used for UC9.3 path-of-least-travel: when assigning a new chute we sort
# destinations starting from the station immediately downstream of where
# the item was inducted rather than always from the global minimum.
#
# induction_1: first scan at entry to sorter (station 701)
# induction_2: re-scan for items already on sorter (also feeds into 701)
# DP11/12/13:  downstream decision points before each packout section
# ---------------------------------------------------------------------------
STATION_ID_PATH = {
	'induction_1': 701,
	'induction_2': 701,
	'DP11':        801,
	'DP12':        901,
	'DP13':        1001,
}


# ===========================================================================
# LEVEL 3 SHIP
# ===========================================================================

class Level_3_Ship_OrderRouting(
	EuroSorterContentTracking,
	EuroSorterPermissivePolling,
	EuroSorterPolling,
	EuroSorterAccessWCS,
	EuroSorterLightControl,
):
	"""
	Routing class for the Level3_Ship EuroSort sorter.

	Permissives:  [EuroSort]EuroSort/Level3_Ship/Control/
	Destinations: [EuroSort]EuroSort/Level3_Ship/Destinations/

	Flipper lockout
	  Physical constraint — 4-carrier gap required before switching to a
	  different position (chute or side) at the same station. Same position
	  is always safe. Tracked in _station_carrier_log keyed by station int.

	Path of least travel (UC9.3)
	  When assigning a new chute, destinations are sorted starting from the
	  station immediately downstream of the item's induction point
	  (STATION_ID_PATH). Items inducted at DP11 (station 801) will prefer
	  chutes in the 800 series before wrapping forward to 900, 1000, 700, etc.
	"""

	STATION_CARRIER_CACHE_SCOPE = 'EuroSort-L3Ship-StationCarrier'

	CONTROL_PERMISSIVE_TAG_MAPPING = {
		'max_noread_recirc':               'noread recirc attempts',
		'max_resort_recirc':               'excessive recirc attempts',
		'squelch_wcs_updates':             'Squelch WCS',
		'reset_dict':                      'clear_defaults',
		'reload_state':                    'Reload Routes',

		'ob_configuration':                _OB + 'ob_configuration',
		'ob_chute_limit':                  _OB + 'ob_chute_limit',

		'packout_configuration':           _PACKOUT + 'packout_configuration',
		'tray_utilization_threshold_pct':  _PACKOUT + 'tray_utilization_threshold_pct',
		'chute_utilization_threshold_pct': _PACKOUT + 'chute_utilization_threshold_pct',
		'reset_utilization_diff':          _PACKOUT + 'reset_utilization_diff',
		'rear_chute_active':               _PACKOUT + 'rear_chute_active',
		'routing_to_ob_active':            _PACKOUT + 'routing_to_ob_active',
		'inspection_active':               _PACKOUT + 'inspection_active',

		'purge_active':                    _PURGE + 'purge_active',
		'purge_reset_to_normal':           _PURGE + 'purge_reset_to_normal',

		'order_aging':                     _AGING + 'order_aging',
		'bag_dims':                        _DIMS + 'bag/dims',
	}

	DEST_STATUS_TAGS = {
		'in_service': 'In_Service',
		'faulted':    'Faulted',
		'dfs':        'DFS',
		'ofs':        'OFS',
		'status':     'Light/Status',
	}

	def __init__(self, name, **init_cfg):
		super(Level_3_Ship_OrderRouting, self).__init__(name, **init_cfg)

		self.logger         = Logger(name)
		self.DEST_BASE_PATH = '[EuroSort]EuroSort/%s/Destinations' % name
		self.scanner_id     = None

		for perm, tag in self.CONTROL_PERMISSIVE_TAG_MAPPING.items():
			self._subscribe_control_permissive(perm, tag)

		self._polling_methods.append(self._refresh_destination_status_from_tags)
		self._polling_methods.append(self._check_utilization_thresholds)

		self._init_polling()

		if self._gp('reset_dict', False):
			self.clear_all_destinations()

	# ------------------------------------------------------------------
	# Small helpers
	# ------------------------------------------------------------------

	def _gp(self, name, default=None):
		try:
			return self.get_permissive(name)
		except Exception:
			return default

	def _safe_tag_write(self, paths, values):
		try:
			if isinstance(paths, (str, unicode)):
				paths = [paths]
			if not isinstance(values, (list, tuple)):
				values = [values]
			system.tag.writeBlocking(paths, values)
		except Exception as e:
			try:
				self.logger.error('Tag write failed for %s: %s' % (paths, e))
			except Exception:
				pass

	def _control_tag_path(self, tag_name):
		return '[EuroSort]EuroSort/%s/Control/%s' % (self.name, tag_name)

	def _write_permissive(self, perm_name, value):
		tag_suffix = self.CONTROL_PERMISSIVE_TAG_MAPPING.get(perm_name)
		if not tag_suffix:
			return
		self._safe_tag_write(self._control_tag_path(tag_suffix), value)

	def _dest_is_eligible(self, rec):
		"""True if a destination record is in a routable state."""
		if rec is None:
			return False
		if not rec.get('in_service', True):
			return False
		if rec.get('faulted', False):
			return False
		if self._dest_get(rec, 'dfs', False):
			return False
		if self._dest_get(rec, 'ofs', False):
			return False
		return True

	def _is_noread(self, ibn):
		return str(ibn or '').lower() in ('noread', 'noscan', 'nocode', '')

	# ------------------------------------------------------------------
	# Station carrier flipper lockout
	# ------------------------------------------------------------------

	@property
	def _station_carrier_log(self):
		"""
		Dict: station_int -> {'carrier': int, 'chute': str, 'side': str}
		Tracks the last carrier assigned at each station so the flipper
		lockout can be enforced when a different position is requested.
		"""
		try:
			return ExtraGlobal.access(self.name, self.STATION_CARRIER_CACHE_SCOPE)
		except KeyError:
			log = {}
			ExtraGlobal.stash(log, self.name, self.STATION_CARRIER_CACHE_SCOPE,
			                  lifespan=60 * 60 * 24)
			return log

	def _record_station_carrier(self, dest_key, carrier_number):
		"""
		Records that carrier_number was assigned to dest_key.
		Called after a successful assignment.
		"""
		parts = str(dest_key).split('-')
		if len(parts) != 5:
			return
		_, station, chute, _dest, side = parts
		self._station_carrier_log[int(station)] = {
			'carrier': int(carrier_number),
			'chute':   chute,
			'side':    side,
		}

	def _is_station_safe_for_carrier(self, dest_key, carrier_number):
		"""
		Returns True if carrier_number can be assigned to dest_key without
		violating the 4-carrier flipper lockout.

		Logic:
		  - No prior assignment at this station         -> OK
		  - Same chute AND same side as last assignment -> OK (flipper already set)
		  - Different chute OR different side           -> need gap >= 4
		"""
		parts = str(dest_key).split('-')
		if len(parts) != 5:
			return True

		_, station, chute, _dest, side = parts
		station_int = int(station)

		log = self._station_carrier_log.get(station_int)
		if not log:
			return True

		last_carrier = int(log.get('carrier', 0))
		last_chute   = str(log.get('chute', ''))
		last_side    = str(log.get('side',  ''))

		# Same position on the plane — flipper is already set, always safe
		if chute == last_chute and side == last_side:
			return True

		# Different chute or different side — enforce 4-carrier gap
		gap = int(carrier_number) - last_carrier
		return gap >= CARRIER_FLIPPER_LOCKOUT

	# ------------------------------------------------------------------
	# Path-of-least-travel sorting (UC9.3)
	# ------------------------------------------------------------------

	def _start_station_for_scanner(self, scanner_id):
		"""
		Returns the nearest downstream packout station for a given scanner_id.
		Falls back to the global minimum station if scanner_id is unknown.
		"""
		if scanner_id and scanner_id in STATION_ID_PATH:
			return STATION_ID_PATH[scanner_id]
		# Default: start from the lowest station on the sorter
		return 0

	def _sorted_destinations_from(self, start_station):
		"""
		UC9.3 — Returns destination keys sorted by path-of-least-travel
		starting from start_station.

		Items travel in ascending station order and wrap around. A carrier
		inducted at station 801 (DP11) will reach station 801 first, then
		802 ... 1040, then wrap to 701, 702 ... 800.

		Sort key: (station - start_station) % max_station
		This gives correct wrap-around ordering without needing to know the
		physical loop length — stations not in the range simply sort last.
		"""
		# Use a large modulus so unknown stations always sort after known ones
		MAX_STATION = 10000

		def sort_key(dest_key):
			try:
				parts = dest_key.split('-')
				station = int(parts[1])
				chute   = int(parts[2])
				dest    = int(parts[3])
				side    = parts[4]
				# Wrap-around distance from start_station
				distance = (station - start_station) % MAX_STATION
				return (distance, chute, dest, side)
			except Exception:
				return (MAX_STATION, 9, 9, dest_key)

		return sorted(self._destination_contents.keys(), key=sort_key)

	# ------------------------------------------------------------------
	# Destination clear
	# ------------------------------------------------------------------

	def clear_all_destinations(self):
		try:
			self._initialize_destination_contents(full_clear=True)
			self.logger.info('Reinitialized destination contents for %s' % self.name)
		except Exception as e:
			self.logger.warn('Failed reinitializing destination contents: %s' % str(e))

		updated = 0
		for dest_key in list(self.destinations_all_transit_info().keys()):
			try:
				self.clear_level3_ship_occupancy(dest_key)
				updated += 1
			except Exception as e:
				self.logger.warn('Failed clearing destination %s: %s' % (dest_key, str(e)))

		try:
			ExtraGlobal.trash(self.name, self.STATION_CARRIER_CACHE_SCOPE)
		except KeyError:
			pass

		self.logger.info('clear_all_destinations: reset %d chutes' % updated)
		return {'ok': True, 'data': {'updated': updated}, 'message': None}

	# ------------------------------------------------------------------
	# Destination status polling
	# ------------------------------------------------------------------

	def _destination_status_tagpaths(self, dest_key):
		base   = self.DEST_BASE_PATH
		prefix = '%s/%s/Destination' % (base, dest_key)
		return {
			fn: '%s/%s' % (prefix, tag)
			for fn, tag in self.DEST_STATUS_TAGS.items()
		}

	def _refresh_destination_status_from_tags(self):
		"""Periodic — syncs In_Service, Faulted, DFS, OFS from UDT tags into cache."""
		try:
			all_dest = list(self.destinations_all_transit_info().keys())
		except Exception:
			return
		if not all_dest:
			return

		read_paths, meta = [], []
		for dest_key in all_dest:
			for field_name, path in self._destination_status_tagpaths(dest_key).items():
				if field_name == 'status':
					continue
				read_paths.append(path)
				meta.append((dest_key, field_name))

		if not read_paths:
			return

		results = system.tag.readBlocking(read_paths)
		updates_by_dest = {}
		for (dest_key, field_name), r in zip(meta, results):
			try:
				q = getattr(r, 'quality', None)
				if q is not None and not q.isGood():
					continue
				updates_by_dest.setdefault(dest_key, {})[field_name] = bool(r.value)
			except Exception:
				continue

		for dest_key, updates in updates_by_dest.items():
			current = self.destination_get(dest_key) or {}
			common_updates, chute_updates = {}, {}
			for k, v in updates.items():
				if k in ('dfs', 'ofs'):
					if self._dest_get(current, k) != v:
						chute_updates[k] = v
				else:
					if current.get(k) != v:
						common_updates[k] = v
			if common_updates or chute_updates:
				self._dest_update(dest_key, common_updates, chute_updates)

	# ------------------------------------------------------------------
	# Utilization monitoring — rear_chute_active + routing_to_ob_active
	# ------------------------------------------------------------------

	def _check_utilization_thresholds(self):
		"""
		Periodic — evaluates both thresholds each poll cycle with hysteresis.

		rear_chute_active (UC9.7):
		  ON  when front_pct > chute_utilization_threshold_pct
		  OFF when front_pct < (threshold - reset_utilization_diff)

		routing_to_ob_active (UC1.2):
		  ON  when carrier_pct > tray_utilization_threshold_pct
		  OFF when carrier_pct < (threshold - reset_utilization_diff)
		"""
		reset_diff = float(self._gp('reset_utilization_diff', 10.0) or 10.0)

		# rear_chute_active
		chute_threshold = float(self._gp('chute_utilization_threshold_pct', 80.0) or 80.0)
		front_pct       = self._front_chute_utilization_pct()
		rear_active     = bool(self._gp('rear_chute_active', False))

		if not rear_active and front_pct > chute_threshold:
			self.logger.info('rear_chute_active ON — front %.1f%% > %.1f%%' % (front_pct, chute_threshold))
			self._write_permissive('rear_chute_active', True)
		elif rear_active and front_pct < (chute_threshold - reset_diff):
			self.logger.info('rear_chute_active OFF — front %.1f%% < %.1f%%' % (front_pct, chute_threshold - reset_diff))
			self._write_permissive('rear_chute_active', False)

		# routing_to_ob_active
		tray_threshold = float(self._gp('tray_utilization_threshold_pct', 75.0) or 75.0)
		carrier_pct    = self.carrier_usage_percent()
		ob_active      = bool(self._gp('routing_to_ob_active', False))

		if not ob_active and carrier_pct > tray_threshold:
			self.logger.info('routing_to_ob_active ON — carrier %.1f%% > %.1f%%' % (carrier_pct, tray_threshold))
			self._write_permissive('routing_to_ob_active', True)
		elif ob_active and carrier_pct < (tray_threshold - reset_diff):
			self.logger.info('routing_to_ob_active OFF — carrier %.1f%% < %.1f%%' % (carrier_pct, tray_threshold - reset_diff))
			self._write_permissive('routing_to_ob_active', False)

	def _front_chute_utilization_pct(self):
		"""
		% of FRONT pack-out positions (has_front_rear=True, in-service, non-faulted)
		that are currently occupied. Used to gate rear_chute_active.
		"""
		total = occupied = 0
		for _key, rec in self._destination_contents.items():
			if rec is None:
				continue
			chute_info = self._dest_info(rec)
			if not chute_info.get('has_front_rear', rec.get('has_front_rear', False)):
				continue
			if rec.get('position') != 'FRONT':
				continue
			if not rec.get('in_service', True):
				continue
			if rec.get('faulted', False):
				continue
			total += 1
			if bool(rec.get('occupied', False)):
				occupied += 1
		if total == 0:
			return 0.0
		return round((occupied / float(total)) * 100.0, 2)

	# ==================================================================
	# route_destination
	# ==================================================================

	def route_destination(self, carrier_number, ibn, wcs_data=None, scanner_id=None):
		"""
		UC1 — Main entry point. Called on every induction scan.

		scanner_id is the induction point name (e.g. 'induction_1', 'DP11').
		It is stored on self.scanner_id and forwarded to _find_consolidation_chute
		so UC9.3 path-of-least-travel sorts destinations relative to the nearest
		downstream station for that induction point (STATION_ID_PATH).

		Sequence:
		  1. Purge active     -> _route_purge (Bryor)
		  2. NoRead barcode   -> _route_noread
		  3. Carrier already has a valid assignment -> return it
		  4. Resolve IBN via get_l3ship_ibn_info aggregation
		  5. No valid zone/order -> jackpot
		  6. hold_inspect + inspection_active -> INSPECTION chute
		  7. MST/MSQ status -> _route_high_priority
		  8. Normal consolidation -> _route_order
		  9. No consolidation chute available -> _route_ob_check
		"""
		carrier_number = int(carrier_number)

		# Store scanner_id so helpers can reference induction point
		if scanner_id is not None:
			self.scanner_id = scanner_id

		# 1. Purge
		if self._is_purge_active():
			return self._route_purge(carrier_number)

		# 2. NoRead
		if self._is_noread(ibn):
			return self._route_noread(carrier_number, ibn)

		# 3. Existing valid destination
		existing = self._get_existing_carrier_destination(carrier_number)
		if existing:
			return existing

		# 4. Resolve IBN from WCS
		ibn_info = self.get_l3ship_ibn_info(ibn)

		if not ibn_info:
			self.logger.info('route_destination: no valid zone/order for ibn=%s — jackpot' % ibn)
			self.log_event('Routing', reason='No WCS match for ibn=%s' % ibn, ibn=ibn, code=100)
			return self._get_jackpot_dest(carrier_number)

		# Stash on carrier so handle_verify can access without re-querying
		self.carrier_update(
			carrier_number,
			issue_info    = ibn_info,
			assigned_name = ibn_info.get('order_number'),
			assigned_mode = 'L3SHIP',
			track_id      = ibn_info.get('ibn'),
		)

		order_status = str(ibn_info.get('status', '')).lower()

		# 5. Inspection (UC9.10)
		if bool(ibn_info.get('hold_inspect', False)) and bool(self._gp('inspection_active', False)):
			dest = self._route_inspection(carrier_number, ibn_info)
			if dest:
				return dest

		# 6. High priority (UC9.4)
		if order_status in ('mst', 'msq'):
			dest = self._route_high_priority(carrier_number, ibn_info)
			if dest:
				return dest

		# 7. Normal consolidation
		dest = self._route_order(carrier_number, ibn_info)
		if dest:
			return dest

		# 8. No consolidation chute — check OB eligibility
		return self._route_ob_check(carrier_number, ibn_info)

	def _get_existing_carrier_destination(self, carrier_number):
		"""
		Returns the carrier's current destination if it is still valid
		(in-service, not faulted, not DFS/OFS). Clears it if invalid.
		"""
		rec = self.carrier_get(carrier_number)
		if not rec:
			return None
		dest = rec.get('destination')
		if not dest:
			return None
		dest_rec = self.destination_get(dest)
		if self._dest_is_eligible(dest_rec):
			return dest
		# Destination no longer valid — clear so we re-route
		self.carrier_update(carrier_number, destination=None)
		return None

	def _get_jackpot_dest(self, carrier_number):
		"""Returns the first available JACKPOT chute dest_key, or None."""
		for dest_key, rec in self._destination_contents.items():
			if rec is None:
				continue
			chute_type = str(rec.get('chute_type', '')).upper()
			if chute_type not in ('JACKPOT', 'NOREAD'):
				continue
			if not self._dest_is_eligible(rec):
				continue
			return dest_key
		self.logger.warn('_get_jackpot_dest: no JACKPOT chute available for carrier %s' % carrier_number)
		return None

	# ==================================================================
	# _route_order
	# ==================================================================

	def _route_order(self, carrier_number, ibn_info):
		"""
		UC1.1 — Finds a consolidation chute for the given order and assigns
		the carrier to it.

		Returns dest_key string or None if no valid chute is available.
		"""
		order_number = ibn_info.get('order_number')
		ibns         = ibn_info.get('ibns') or []
		expected     = int(ibn_info.get('expected_count', 0))

		dest = self._find_consolidation_chute(
			ibn_info,
			carrier_number = carrier_number,
			scanner_id     = self.scanner_id,
		)

		if not dest:
			self.log_event('Routing',
				reason='No consolidation chute for order=%s ibn=%s' % (order_number, ibn_info.get('ibn')),
				ibn=ibn_info.get('ibn'), code=5,
			)
			return None

		# Assign carrier -> destination
		self.assign_carrier_to_destination(
			carrier_number  = carrier_number,
			dest_identifier = dest,
			assigned_name   = order_number,
			assigned_mode   = 'L3SHIP',
			transit_info    = ibn_info,
		)

		# Record station assignment for flipper lockout
		self._record_station_carrier(dest, carrier_number)

		# Set expected_line_count on the chute if not already set
		dest_rec   = self.destination_get(dest) or {}
		chute_info = dest_rec.get('chute_info') or {}
		if int(chute_info.get('expected_line_count', 0) or 0) == 0 and expected:
			self.destination_update(dest, expected_line_count=expected, missing_ibns=ibns)

		self.notify_wcs_l3ship_item_inducted(ibn_info.get('ibn'), dest)

		self.log_event('Routing',
			reason='Assigned ibn=%s order=%s to %s' % (ibn_info.get('ibn'), order_number, dest),
			ibn=ibn_info.get('ibn'), destination=dest, code=4,
		)

		return dest

	# ==================================================================
	# _find_consolidation_chute
	# ==================================================================

	def _find_consolidation_chute(self, ibn_info, carrier_number=None, exclude=None, scanner_id=None):
		"""
		Finds the best available consolidation chute for the given order.

		Rules applied in order:
		  Chute in_service
		  Chute not OB / BAGGING / JACKPOT
		  Chute not DFS / OFS / Faulted
		  UC9.2  — A chute may not contain two orders with the same sort code
		  UC9.1  — Position must be below max_orders_per_position
		  UC9.7  — Rear positions only when rear_chute_active=True AND the
		           front of the same physical chute is fully consolidated
		  UC9.3  — Path of least travel: destinations sorted from the nearest
		           downstream station for the given scanner_id (STATION_ID_PATH).
		           Falls back to global ascending sort when scanner_id is unknown.
		  Flipper lockout — different position at same station needs 4-carrier gap

		SHARED INTERFACE: Charles calls this from _ob_release_assign_all_orders.
		Signature: (ibn_info, carrier_number=None, exclude=None, scanner_id=None)

		Args:
		    ibn_info:       dict from get_l3ship_ibn_info
		    carrier_number: int — required for lockout check; None skips lockout
		    exclude:        set of dest_keys to skip (OB release cycle dedup)
		    scanner_id:     induction point name from STATION_ID_PATH; drives
		                    UC9.3 sort order

		Returns:
		    dest_key string of the best available position, or None.
		"""
		order_number = ibn_info.get('order_number')
		sort_code    = str(ibn_info.get('consol_subzone', ''))

		packout_cfg = self._gp('packout_configuration') or {}
		max_orders  = int(packout_cfg.get('max_order_count', 2) or 2)
		rear_active = bool(self._gp('rear_chute_active', False))
		exclude     = exclude or set()

		# UC9.3 — sort from nearest downstream station for this induction point
		start_station = self._start_station_for_scanner(scanner_id or self.scanner_id)
		ordered_dests = self._sorted_destinations_from(start_station)

		best_front = None
		best_rear  = None

		for dest_key in ordered_dests:
			if dest_key in exclude:
				continue

			rec = self.destination_get(dest_key)
			if rec is None:
				continue

			# Only NORMAL and HP chutes accept consolidation items
			chute_type = str(rec.get('chute_type', '')).upper()
			if chute_type not in CONSOLIDATION_CHUTE_TYPES:
				continue

			if not self._dest_is_eligible(rec):
				continue

			# Must have front/rear positions (OB and BAGGING do not)
			chute_info = rec.get('chute_info') or {}
			if not chute_info.get('has_front_rear', rec.get('has_front_rear', False)):
				continue

			position = rec.get('position')   # 'FRONT' or 'REAR'

			# UC9.7 — rear only when rear_chute_active + front consolidated
			if position == 'REAR':
				if not rear_active:
					continue
				front_key = self._front_key_for(dest_key)
				if front_key:
					front_rec  = self.destination_get(front_key) or {}
					front_info = front_rec.get('chute_info') or {}
					if not bool(front_info.get('ready_for_packout', False)):
						continue
					if not front_rec.get('occupied', False):
						continue

			# UC9.2 — reject if sort code already present in this chute
			if sort_code and self.chute_has_sort_code(dest_key, sort_code):
				continue

			# Re-entry — same order already assigned here, safe to reuse
			existing_orders = chute_info.get('orders') or []
			order_numbers   = [o.get('order_number') for o in existing_orders if isinstance(o, dict)]
			if order_number in order_numbers:
				if position == 'FRONT':
					return dest_key
				if position == 'REAR' and rear_active:
					return dest_key
				continue

			# UC9.1 — max orders per position
			if len(existing_orders) >= max_orders:
				continue

			# Flipper lockout
			if carrier_number is not None:
				if not self._is_station_safe_for_carrier(dest_key, carrier_number):
					continue

			# Candidate passes all rules
			if position == 'FRONT' and best_front is None:
				best_front = dest_key
				break   # First valid FRONT in path order is optimal (UC9.3)

			if position == 'REAR' and best_rear is None:
				best_rear = dest_key

		# UC9.7 — prefer FRONT; rear only if no front available
		return best_front or best_rear

	def _front_key_for(self, rear_dest_key):
		"""
		Given a REAR dest_key (dest digit == 1), returns the corresponding
		FRONT dest_key (dest digit == 2) for the same physical chute.
		Returns None if the dest_key cannot be parsed.
		"""
		parts = str(rear_dest_key).split('-')
		if len(parts) != 5 or parts[3] != '1':
			return None
		_, station, chute, _dest, side = parts
		return 'DST-%s-%s-2-%s' % (station, chute, side)

	# ==================================================================
	# _route_high_priority
	# ==================================================================

	def _route_high_priority(self, carrier_number, ibn_info):
		"""
		UC9.4 — Diverts to the nearest HP-configured chute.

		Nearest = lowest station downstream of the induction point (UC9.3)
		with an available HP chute that passes sort-code, max-orders, and
		flipper-lockout checks.
		Falls through to None if no HP chute is available.
		"""
		order_number  = ibn_info.get('order_number')
		sort_code     = str(ibn_info.get('consol_subzone', ''))
		packout_cfg   = self._gp('packout_configuration') or {}
		max_orders    = int(packout_cfg.get('max_order_count', 2) or 2)

		start_station = self._start_station_for_scanner(self.scanner_id)
		ordered_dests = self._sorted_destinations_from(start_station)

		for dest_key in ordered_dests:
			rec = self.destination_get(dest_key)
			if rec is None:
				continue

			if str(rec.get('chute_type', '')).upper() != 'HP':
				continue

			if not self._dest_is_eligible(rec):
				continue

			if rec.get('position') != 'FRONT':
				continue

			chute_info = rec.get('chute_info') or {}

			if sort_code and self.chute_has_sort_code(dest_key, sort_code):
				continue

			if len(chute_info.get('orders') or []) >= max_orders:
				continue

			if carrier_number is not None:
				if not self._is_station_safe_for_carrier(dest_key, carrier_number):
					continue

			self.assign_carrier_to_destination(
				carrier_number  = carrier_number,
				dest_identifier = dest_key,
				assigned_name   = order_number,
				assigned_mode   = 'L3SHIP-HP',
				transit_info    = ibn_info,
			)
			self._record_station_carrier(dest_key, carrier_number)
			self.notify_wcs_l3ship_item_inducted(ibn_info.get('ibn'), dest_key)

			self.log_event('Routing',
				reason='HP route ibn=%s order=%s to %s' % (ibn_info.get('ibn'), order_number, dest_key),
				ibn=ibn_info.get('ibn'), destination=dest_key, code=9,
			)
			return dest_key

		return None

	# ==================================================================
	# _route_noread
	# ==================================================================

	def _route_noread(self, carrier_number, ibn):
		"""
		Handles NoRead barcodes.

		Increments recirculation count. When max_noread_recirc is reached,
		diverts to a JACKPOT chute and notifies WCS.
		"""
		carrier_number = int(carrier_number)
		rec            = self.carrier_get(carrier_number) or {}
		recirc_count   = int(rec.get('recirculation_count', 0) or 0) + 1
		max_count      = int(self._gp('max_noread_recirc', 0) or 0)

		self.carrier_update(carrier_number, recirculation_count=recirc_count)

		self.logger.info('_route_noread carrier=%s recirc=%s max=%s' % (carrier_number, recirc_count, max_count))

		if max_count > 0 and recirc_count >= max_count:
			dest = self._get_jackpot_dest(carrier_number)
			if dest:
				self.assign_carrier_to_destination(
					carrier_number  = carrier_number,
					dest_identifier = dest,
					assigned_name   = 'JACKPOT',
					assigned_mode   = 'NOREAD',
					transit_info    = {'ibn': str(ibn), 'reason': 'noread_max_recirc'},
				)
				self.notify_wcs_l3ship_jackpot_divert(str(ibn), None, dest)
				self.log_event('Routing',
					reason='NoRead max recirc ibn=%s to %s' % (ibn, dest),
					ibn=str(ibn), destination=dest, code=10,
				)
				return dest

		return None

	# ==================================================================
	# _route_ob_check
	# ==================================================================

	def _route_ob_check(self, carrier_number, ibn_info):
		"""
		Decides whether to send an item to an OB chute.

		Two conditions must BOTH be true to divert to OB:
		  1. routing_to_ob_active is True
		  2. ob_reinducted == False (UC5.6 — once re-inducted, never OB again)
		"""
		carrier_number = int(carrier_number)
		rec            = self.carrier_get(carrier_number) or {}

		ob_active     = bool(self._gp('routing_to_ob_active', False))
		ob_reinducted = bool(rec.get('ob_reinducted', False))

		if ob_active and not ob_reinducted:
			dest = self._ob_select_chute()
			if dest:
				self.assign_carrier_to_destination(
					carrier_number  = carrier_number,
					dest_identifier = dest,
					assigned_name   = ibn_info.get('order_number'),
					assigned_mode   = 'OB',
					transit_info    = ibn_info,
				)
				self.notify_wcs_l3ship_ob_divert(
					ibn           = ibn_info.get('ibn'),
					from_dest_key = None,
					ob_dest_key   = dest,
				)
				self.log_event('Routing',
					reason='OB divert ibn=%s to %s' % (ibn_info.get('ibn'), dest),
					ibn=ibn_info.get('ibn'), destination=dest, code=6,
				)
				return dest

		return self._max_recirc(carrier_number, ibn_info)

	# ==================================================================
	# _max_recirc
	# ==================================================================

	def _max_recirc(self, carrier_number, ibn_info):
		"""
		Hard recirculation limit enforcement. When reached, item is forced
		to JACKPOT regardless of routing_to_ob_active state.
		"""
		carrier_number = int(carrier_number)
		rec            = self.carrier_get(carrier_number) or {}
		recirc_count   = int(rec.get('recirculation_count', 0) or 0) + 1
		max_recirc     = int(self._gp('max_resort_recirc', 0) or 0)

		self.carrier_update(carrier_number, recirculation_count=recirc_count)

		self.logger.info('_max_recirc carrier=%s recirc=%s max=%s' % (carrier_number, recirc_count, max_recirc))

		if max_recirc > 0 and recirc_count >= max_recirc:
			ibn  = ibn_info.get('ibn', '')
			dest = self._get_jackpot_dest(carrier_number)
			if dest:
				self.assign_carrier_to_destination(
					carrier_number  = carrier_number,
					dest_identifier = dest,
					assigned_name   = 'JACKPOT',
					assigned_mode   = 'MAX_RECIRC',
					transit_info    = ibn_info,
				)
				self.notify_wcs_l3ship_jackpot_divert(ibn, None, dest)
				self.log_event('Routing',
					reason='Max recirc ibn=%s to jackpot %s' % (ibn, dest),
					ibn=ibn, destination=dest, code=11,
				)
				return dest

		return None

	# ==================================================================
	# UC9.10 — _route_inspection
	# ==================================================================

	def _route_inspection(self, carrier_number, ibn_info):
		"""
		Routes a hold_inspect item to an INSPECTION chute.
		Returns dest_key or None if no INSPECTION chute is available.
		"""
		order_number  = ibn_info.get('order_number')
		start_station = self._start_station_for_scanner(self.scanner_id)
		ordered_dests = self._sorted_destinations_from(start_station)

		for dest_key in ordered_dests:
			rec = self.destination_get(dest_key)
			if rec is None:
				continue
			if str(rec.get('chute_type', '')).upper() != 'INSPECTION':
				continue
			if not self._dest_is_eligible(rec):
				continue
			if carrier_number is not None:
				if not self._is_station_safe_for_carrier(dest_key, carrier_number):
					continue

			self.assign_carrier_to_destination(
				carrier_number  = carrier_number,
				dest_identifier = dest_key,
				assigned_name   = order_number,
				assigned_mode   = 'INSPECTION',
				transit_info    = ibn_info,
			)
			self._record_station_carrier(dest_key, carrier_number)
			self.notify_wcs_l3ship_item_inducted(ibn_info.get('ibn'), dest_key)

			self.log_event('Routing',
				reason='Inspection route ibn=%s order=%s to %s' % (ibn_info.get('ibn'), order_number, dest_key),
				ibn=ibn_info.get('ibn'), destination=dest_key, code=8,
			)
			return dest_key

		return None

	# ==================================================================
	# handle_verify  (discharge message handler)
	# ==================================================================

	def handle_verify(self, carrier_number, dest_key, verify_data=None):
		"""
		UC9 — Called when EuroSort reports a discharge message for a carrier.

		MessageCode dispatch:
		  DISCHARGE_ATTEMPTED           -> mark_carrier_attempted
		  DISCHARGED_AT_DESTINATION     -> _finalize_discharge + mark_carrier_delivered
		  DISCHARGE_FAILED              -> mark_carrier_failed
		  DISCHARGE_ABORTED_*           -> mark_carrier_aborted
		  Everything else               -> mark_carrier_unknown
		"""
		carrier_number = int(carrier_number)
		message_code   = getattr(verify_data, 'message_code', None) if verify_data else None

		rec      = self.carrier_get(carrier_number) or {}
		ibn_info = rec.get('issue_info') or {}
		ibn      = ibn_info.get('ibn', '')

		self.log_event('Routing',
			reason='handle_verify carrier=%s dest=%s code=%s' % (carrier_number, dest_key, message_code),
			ibn=ibn, destination=dest_key, code=99,
		)

		if message_code == MessageCode.DISCHARGE_ATTEMPTED:
			if not rec.get('discharged_attempted', False):
				self.mark_carrier_attempted(carrier_number)

		elif message_code == MessageCode.DISCHARGED_AT_DESTINATION:
			self._finalize_discharge(carrier_number, dest_key, ibn_info)
			self.mark_carrier_delivered(carrier_number)

		elif message_code == MessageCode.DISCHARGE_FAILED:
			self.mark_carrier_failed(carrier_number)

		elif message_code in (
			MessageCode.DISCHARGE_ABORTED_DESTINATION_FULL,
			MessageCode.DISCHARGE_ABORTED_POSITIONING_ERROR,
		):
			recirc = int(rec.get('recirculation_count', 0) or 0) + 1
			self.carrier_update(carrier_number, recirculation_count=recirc)
			self.mark_carrier_aborted(carrier_number)

		else:
			self.mark_carrier_unknown(carrier_number)

	# ==================================================================
	# UC9.8 / UC9.9 — _finalize_discharge
	# ==================================================================

	def _finalize_discharge(self, carrier_number, dest_key, ibn_info=None):
		"""
		Steps:
		  1. Add IBN to chute ibns list, remove from missing_ibns
		  2. Update counts (item_count_total, line_count_total, percent_consolidated)
		  3. Update oldest_order_age_sec from first_item_delivered_ts
		  4. Evaluate ready_for_packout (UC9.9) — all expected IBNs delivered
		  5. If ready_for_packout and position is REAR, set drop_pending=True
		     so the WCS layer raises the batch door (UC9.8). door_state goes UP.
		  6. If ready_for_packout and position is FRONT and chute has a front door
		     (OB), set drop_pending=True so the WCS layer opens the front door
		     for discharge (UC2.1). door_state goes DOWN.
		  7. Notify WCS
		"""
		ibn_info      = ibn_info or {}
		ibn           = str(ibn_info.get('ibn', ''))
		order_number  = str(ibn_info.get('order_number', ''))
		expected_ibns = ibn_info.get('ibns') or []
		expected_cnt  = int(ibn_info.get('expected_count', 0) or 0)

		dest_rec   = self.destination_get(dest_key) or {}
		chute_info = dest_rec.get('chute_info') or {}

		# ── 1. Update IBN tracking ────────────────────────────────────
		current_ibns = list(chute_info.get('ibns') or [])
		missing_ibns = list(chute_info.get('missing_ibns') or expected_ibns)
		item_count   = int(chute_info.get('item_count_total', 0) or 0)
		line_count   = int(chute_info.get('line_count_total', 0) or 0)

		if ibn and ibn not in current_ibns:
			current_ibns.append(ibn)
			item_count += 1
			line_count += 1

		if ibn in missing_ibns:
			missing_ibns.remove(ibn)

		# ── 2. Percent consolidated ───────────────────────────────────
		if expected_cnt > 0:
			delivered_cnt = expected_cnt - len(missing_ibns)
			pct           = round((delivered_cnt / float(expected_cnt)) * 100.0, 2)
		else:
			pct = 0.0

		# ── 3. Age tracking ───────────────────────────────────────────
		first_ts = dest_rec.get('first_item_delivered_ts')
		if first_ts:
			try:
				age_sec = int(system.date.secondsBetween(first_ts, system.date.now()))
			except Exception:
				age_sec = 0
		else:
			age_sec = 0

		# ── 4. ready_for_packout (UC9.9) ──────────────────────────────
		ready    = len(missing_ibns) == 0 and expected_cnt > 0
		position = dest_rec.get('position', '')

		# ── 5 & 6. Door sequencing ────────────────────────────────────
		# drop_pending and door_state are set based on which physical door
		# this chute has, matching request_batch_door_drop / request_ob_release
		# in contents.py. The WCS layer reads Drop_Pending from the UDT tag
		# and performs the physical action.
		drop_pending = False
		door_state   = None   # None means: don't overwrite, let contents.py manage it

		if ready:
			has_batch_door = chute_info.get('has_batch_door', dest_rec.get('has_batch_door', False))
			has_front_door = chute_info.get('has_front_door', dest_rec.get('has_front_door', False))

			if position == 'REAR' and has_batch_door:
				# UC9.8 — all items in rear, raise batch door to drop to front
				drop_pending = True
				door_state   = 'UP'

			elif has_front_door:
				# UC2.1 — OB chute fully consolidated, open front door for discharge
				drop_pending = True
				door_state   = 'DOWN'

		# ── Write back ────────────────────────────────────────────────
		updates = dict(
			ibns                        = current_ibns,
			missing_ibns                = missing_ibns,
			item_count_total            = item_count,
			line_count_total            = line_count,
			percent_orders_consolidated = pct,
			oldest_order_age_sec        = age_sec,
			ready_for_packout           = ready,
			drop_pending                = drop_pending,
			drop_complete               = False,
		)
		if door_state is not None:
			updates['door_state'] = door_state

		self.destination_update(dest_key, updates)

		# ── 7. WCS notifications ──────────────────────────────────────
		if ready:
			if position == 'REAR':
				self.notify_wcs_l3ship_rear_to_front(order_number, dest_key)
			else:
				self.notify_wcs_l3ship_packout_deliver(order_number, dest_key)

		self.log_event('Routing',
			reason='_finalize_discharge dest=%s ibn=%s ready=%s pct=%.1f' % (dest_key, ibn, ready, pct),
			ibn=ibn, destination=dest_key, code=8,
		)

	# ==================================================================
	# Stubs (implemented elsewhere)
	# ==================================================================

	def _ob_select_chute(self):
		"""
		UC4.1 / UC4.2 — Selects the next OB chute using waterfall method
		(highest station to lowest, stopping at ob_chute_limit).
		Charles implements.
		"""
		raise NotImplementedError('Charles — _ob_select_chute (UC4.1, UC4.2)')

	def ob_release(self, dest_key):
		"""UC5.1 / UC5.3 — Charles implements."""
		raise NotImplementedError('Charles — ob_release (UC5.1, UC5.3)')

	def _is_purge_active(self):
		"""Returns True if the system is in purge state (UC12.1)."""
		return bool(self._gp('purge_active', False))

	def _route_purge(self, carrier_number):
		"""UC12.2 / UC12.3 — Bryor implements."""
		raise NotImplementedError('Bryor — _route_purge (UC12.2, UC12.3)')