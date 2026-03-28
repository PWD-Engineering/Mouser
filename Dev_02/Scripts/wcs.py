"""
	WCS feedback routines

"""
from shared.tools.logging import Logger; Logger().trace('Compiling module')

from eurosort.base              import EuroSorterBase
from eurosort.logging           import EventLogging
from eurosort.tracking.contents import Destination,Sides
from eurosort.routing           import EuroSorterRoutingManagement

from shared.data.types.enum import Enum

from database.core import (
	db_select_record,
	db_select_records,
	db_update_record,
	db_insert_record,
	db_delete_record,
	db_bulk_operation,
	WCS_DB
)

import re
import datetime
from java.util import Date



def wcs_timestamp(timestamp=None):
	if timestamp is None:
		timestamp = datetime.datetime.now()
	return timestamp.strftime('%Y%m%d%H%M%S')



class LocationWCS(object):

	# https://regex101.com/r/YxR3G4/1
	WCS_LOCATION_PATTERN = re.compile("""^
		(?P<sorter>[A-Z])
		(?P<station>[0-9]{4})
		(?P<chute>[0-9])
		(?P<dest>[12])
		(?P<side>[AB])
		$""", re.X)

	def parse_wcs_location(self, wcs_location):
		match = self.WCS_LOCATION_PATTERN.match(wcs_location)
		return Destination(**match.groupdict())

	def get_wcs_location_from_destination(self, destination):
		destination = Destination.parse(destination)
		wcs_location = '{wcs_machine_id}{station}{chute}{dest}{side}'.format(
			wcs_machine_id = 'A', # A == Opex
			station        = destination.station,
			side           = destination.side,
		)
		return wcs_location


class MongoWCS(
		LocationWCS,
		EuroSorterBase,
	):

	#-------------------Level 3 Routing---------------------------------------
	def wcs_get_issue(self, barcodes):
		issues = db_select_records('inbound_receipt_info', {'_id': barcodes})
		if not issues:
			return {'_id':barcodes,'zone':'Jackpot','group_id':'-3'}
		if len(issues) == 1:
			return issues[0]
		if len(issues) > 1:
			ibns = []
			for issue in issues:
				ibns.append(issue['_id'])
			return {'_id':','.join(ibns),'zone':'Jackpot','group_id':'-3'}

	def get_chute_info(self, filter_key):
		result = db_select_record('eurosort_lvl3_chutes_db', filter_key)
		if len(result) >= 1:
			return result[0]
		else:
			return None

	def get_chutes(self):
		results = db_select_records('eurosort_lvl3_chutes_db', {})
		if not results:
			return []
		return results

	def get_tr_chutes(self):
		results = db_select_records('eurosort_lvl3_transit_db', {})
		if not results:
			return []
		return results

	def get_carriers(self):
		results = db_select_records('eurosort_lvl3_carrier_info', {})
		if not results:
			return []
		return results

	def pwd_find_matching_chutes(self, filterdict):
		results = db_select_records('eurosort_lvl3_transit_db', filterdict)
		if not results:
			return []
		return results

	def check_chute_status(self):
		filter_dict = {'$and':[
			{'enabled':                True},
			{'occupied':               True},
			{'waiting_for_processing': False},
			{'queued':                 False},
		]}
		results = db_select_records('eurosort_lvl3_chutes_db', filter_dict)
		if not results:
			return None
		return results

	def get_transit_updates(self):
		agg = [
			{'$match':   {'_id': {'$ne': None}}},
			{'$count':   'Items in Transit'},
			{'$project': {'_id': 0}},
		]
		results = db_bulk_operation('eurosort_lvl3_carrier_info', agg)
		if not results:
			return [{'Items in Transit': 0}]
		return results

	def get_chutes_updates(self):
		agg = [
			{
				'$set': {
					'isQueuedForRelease': {'$and': [{'$eq': ['$occupied', True]},  {'$eq': ['$enabled', True]},  {'$eq': ['$queued', True]}]},
					'isAvailable':        {'$and': [{'$eq': ['$occupied', False]}, {'$eq': ['$enabled', True]},  {'$eq': ['$queued', False]}, {'$eq': ['$wcs_processed', True]}]},
					'isToteFull':         {'$and': [{'$eq': ['$occupied', True]},  {'$eq': ['$enabled', True]},  {'$eq': ['$toteFull', True]}]},
					'isChuteFull':        {'$and': [{'$eq': ['$occupied', True]},  {'$eq': ['$enabled', True]},  {'$eq': ['$chuteFull', True]}]},
					'isChuteFaulted':     {'$and': [{'$eq': ['$enabled', True]},   {'$eq': ['$faulted', True]}]},
					'isOccupied':         {'$and': [{'$eq': ['$occupied', True]},  {'$eq': ['$enabled', True]},  {'$not': [{'$in': ['$zone', ['NoRead', 'Jackpot']]}]}]},
					'isProcessing':       {'$and': [{'$eq': ['$waiting_for_processing', True]}, {'$eq': ['$enabled', True]}, {'$eq': ['$occupied', True]}]},
					'isNoRead':           {'$and': [{'$eq': ['$enabled', True]},   {'$eq': ['$zone', 'NoRead']}]},
					'isJackpot':          {'$and': [{'$eq': ['$enabled', True]},   {'$eq': ['$zone', 'Jackpot']}]},
					'isDisabled':         {'$eq': ['$enabled', False]},
				}
			},
			{
				'$group': {
					'_id': None,
					'Queued for Release': {'$sum': {'$cond': ['$isQueuedForRelease', 1, 0]}},
					'Chutes Available':   {'$sum': {'$cond': ['$isAvailable',        1, 0]}},
					'Chutes Tote Full':   {'$sum': {'$cond': ['$isToteFull',         1, 0]}},
					'Chutes Full':        {'$sum': {'$cond': ['$isChuteFull',        1, 0]}},
					'Chutes Faulted':     {'$sum': {'$cond': ['$isChuteFaulted',     1, 0]}},
					'Chutes Occupied':    {'$sum': {'$cond': ['$isOccupied',         1, 0]}},
					'Chutes Processing':  {'$sum': {'$cond': ['$isProcessing',       1, 0]}},
					'NoRead Chutes':      {'$sum': {'$cond': ['$isNoRead',           1, 0]}},
					'Jackpot Chutes':     {'$sum': {'$cond': ['$isJackpot',          1, 0]}},
					'Chutes Disabled':    {'$sum': {'$cond': ['$isDisabled',         1, 0]}},
				}
			},
			{'$project': {'_id': 0}},
		]
		results = db_bulk_operation('eurosort_lvl3_chutes_db', agg)
		return results

	def update_transit_location(self, filterdict, key_columns):
		db_update_record('eurosort_lvl3_transit_db', filterdict, key_columns)

	def update_carrier_info(self, filterdict, key_columns):
		db_upsert_record('eurosort_lvl3_carrier_info', filterdict, key_columns)

	def delete_carrier_info(self, filterdict):
		db_delete_record('eurosort_lvl3_carrier_info', filterdict, key_columns={})

	def get_carrier_info(self, filterdict):
		results = db_select_records('eurosort_lvl3_carrier_info', filterdict)
		if not results:
			return []
		return results

	def insert_carrier_info(self, updates):
		db_insert_record('eurosort_lvl3_carrier_info', updates)

	def _update_chute_info(self, filterdict, key_columns):
		db_update_record('eurosort_lvl3_chutes_db', filterdict, key_columns)

	def get_processing_status(self):
		filter_dict = {'$and': [
			{'occupied':      True},
			{'wcs_processed': True},
		]}
		results = db_select_records('eurosort_lvl3_chutes_db', filter_dict)
		if not results:
			return []
		return results

	#-------------------Level 3 Routing End---------------------------------------

	#-------------------Level 2 Routing ------------------------------------------
	def wcs_lookup(self, barcodes):
		"""
		Try to find issues in inbound_receipt_info (POST) then inbound_receiving_info (PRE).

		Returns
		-------
		(first_code, assigned_name, assigned_mode, router)
		first_code    : primary code used for this decision (string)
		assigned_mode : 'POST', 'PRE', 'UNRESOLVED'
		assigned_name : vendor name (PRE) or zone (POST) or 'JACKPOT'/'UNRESOLVED'
		router        : 'ORDER', 'UNRESOLVED', or 'JACKPOT'
		"""
		queried_codes = ','.join([str(b) for b in (barcodes or [])])
		self.issue_info = self.issue_info or {}

		logger = system.util.getLogger('WCSReturn')

		# ── POST / inbound_receipt_info ───────────────────────────────────────
		issues_from_receipt = db_select_records(
			'inbound_receipt_info',
			{'_id': {'$in': barcodes}}
		) or []

		# ── No POST → try PRE ─────────────────────────────────────────────────
		if len(issues_from_receipt) == 0:
			issues_from_receiving = db_select_records(
				'inbound_receiving_info',
				{'_id': {'$in': barcodes}}
			) or []

			if not issues_from_receiving:
				self.issue_info.update({'codes': queried_codes, 'assigned_mode': 'UNRESOLVED', 'assigned_name': 'UNRESOLVED', 'reason': 'No valid return from WCS (POST or PRE)'})
				return queried_codes, 'UNRESOLVED', 'UNRESOLVED', 'UNRESOLVED'

			if len(issues_from_receiving) > 1:
				vendor_names = list({r.get('vendor_name') for r in issues_from_receiving if r.get('vendor_name')})
				if len(vendor_names) > 1:
					self.issue_info.update({'codes': queried_codes, 'assigned_mode': 'PRE', 'assigned_name': 'JACKPOT', 'reason': 'Found multiple vendors in PRE'})
					logger.info('Returned lookup JACKPOT from PRE for codes: %s' % queried_codes)
					return queried_codes, 'JACKPOT', 'PRE', 'JACKPOT'

			first_rec = issues_from_receiving[0]
			for k, v in first_rec.items():
				self.issue_info['codes' if k == '_id' else k] = v
			self.issue_info.update({'assigned_mode': 'PRE', 'assigned_name': first_rec.get('vendor_name')})
			return first_rec.get('_id'), first_rec.get('vendor_name'), 'PRE', 'ORDER'

		# ── Multiple POST → check zone conflict ──────────────────────────────
		if len(issues_from_receipt) > 1:
			zones = list({r.get('zone') for r in issues_from_receipt if r.get('zone')})
			if len(zones) > 1:
				self.issue_info.update({'codes': queried_codes, 'assigned_name': 'JACKPOT', 'assigned_mode': 'POST', 'reason': 'Found multiple zones in POST'})
				logger.info('Returned lookup JACKPOT from POST for codes: %s' % queried_codes)
				return queried_codes, 'JACKPOT', 'POST', 'JACKPOT'

		# ── Single (or same-zone) POST match ─────────────────────────────────
		first_rec = issues_from_receipt[0]
		ibn     = first_rec.get('_id')
		pre_ibn = first_rec.get('pre_receipt_ibn')

		for k, v in first_rec.items():
			if k in ('_id', 'zone', 'building_id', 'inspect', 'pre_receipt_ibn'):
				self.issue_info['codes' if k == '_id' else k] = v

		if ibn and pre_ibn:
			pre_recs = db_select_records('inbound_receiving_info', {'_id': {'$in': [pre_ibn]}}) or []
			if pre_recs:
				rec_pre = pre_recs[0]
				for k, v in rec_pre.items():
					self.issue_info['codes' if k == '_id' else k] = v
				self.issue_info.update({'assigned_mode': 'PRE', 'assigned_name': rec_pre.get('vendor_name')})
				return rec_pre.get('_id') or ibn, rec_pre.get('vendor_name'), 'PRE', 'ORDER'

		if ibn:
			self.issue_info.update({'assigned_mode': 'POST', 'assigned_name': first_rec.get('zone')})
			return ibn, first_rec.get('zone'), 'POST', 'ORDER'

		self.issue_info.update({'codes': queried_codes, 'assigned_mode': 'UNRESOLVED', 'assigned_name': 'UNRESOLVED', 'reason': 'Unexpected WCS state'})
		return queried_codes, 'UNRESOLVED', 'UNRESOLVED', 'UNRESOLVED'

	#-------------------Level 3 Ship Mongo Queries --------------------------------
	#
	# Single aggregation: IBN -> consol zone -> order -> expected sibling IBNs.
	#
	# Data flow on every induction scan:
	#
	#   ibn scanned
	#     └─► get_l3ship_ibn_info(ibn)
	#             ├─ None  (zone != EU / bad subzone / cancelled / shipped)
	#             │    └─► JACKPOT
	#             ├─ hold_inspect == True
	#             │    └─► INSPECTION chute
	#             └─ valid order_number
	#                  ├─ expected_count  -> set expected_line_count on chute
	#                  └─ ibns list       -> populate missing_ibns on chute

	INVALID_STATUSES_L3SHIP = frozenset(['cancelled', 'shipped'])
	VALID_EU_SUBZONES        = frozenset([1, 2, 3, 4, 5, 6, 7, 8, 9])

	def get_l3ship_ibn_info(self, ibn):
		"""
		Single 5-stage aggregation resolving an IBN to everything routing needs.

		Stage 1 $match  — outbound_scan_sort_ibn: _id==ibn, zone==EU, subzone 1-9
		Stage 2 $lookup — pconsole_issues by IBN (_id), excluding cancelled/shipped
		Stage 3 $unwind — drop document if no valid order record
		Stage 4 $lookup — pconsole_issues by order_number, all active sibling IBNs
		Stage 5 $project — flat output dict

		Returns dict with keys:
		  ibn, consol_zone, consol_subzone, order_number, hold_inspect,
		  status, ibns (list), expected_count (int)
		or None if any stage filters the document out.
		"""
		if not ibn:
			return None

		pipeline = [
			{'$match': {
				'_id':            str(ibn),
				'consol_zone':    'EU',
				'consol_subzone': {'$in': list(self.VALID_EU_SUBZONES)},
			}},
			{'$lookup': {
				'from': 'pconsole_issues',
				'let':  {'ibn': '$_id'},
				'pipeline': [
					{'$match': {
						'$expr':  {'$eq': ['$_id', '$$ibn']},
						'status': {'$nin': list(self.INVALID_STATUSES_L3SHIP)},
					}},
					{'$project': {
						'_id':          1,
						'order_number': 1,
						'hold_inspect': 1,
						'status':       1,
					}},
				],
				'as': 'order_rec',
			}},
			{'$unwind': {
				'path':                       '$order_rec',
				'preserveNullAndEmptyArrays': False,
			}},
			{'$lookup': {
				'from': 'pconsole_issues',
				'let':  {'order_number': '$order_rec.order_number'},
				'pipeline': [
					{'$match': {
						'$expr':  {'$eq': ['$order_number', '$$order_number']},
						'status': {'$nin': list(self.INVALID_STATUSES_L3SHIP)},
					}},
					{'$project': {'_id': 1}},
				],
				'as': 'order_ibns',
			}},
			{'$project': {
				'_id':            0,
				'ibn':            '$_id',
				'consol_zone':    '$consol_zone',
				'consol_subzone': '$consol_subzone',
				'order_number':   '$order_rec.order_number',
				'hold_inspect':   {'$ifNull': ['$order_rec.hold_inspect', False]},
				'status':         '$order_rec.status',
				'ibns':           '$order_ibns._id',
				'expected_count': {'$size': '$order_ibns'},
			}},
		]

		results = db_bulk_operation('outbound_scan_sort_ibn', pipeline)
		if not results:
			return None
		return results[0]

	#-------------------Level 3 Ship Mongo Queries End ----------------------------


class WCSDeviceID(Enum):
	OPEX_EUROSORT          = 19
	OPEX_EUROSORT_RESERVED = 20


class WebserviceWCS(
		EventLogging,
		EuroSorterBase,
	):

	WCS_PROTOCOL = 'http'
	# Prod
#	WCS_HOST     = 'wcs01'
	# Dev
	WCS_HOST     = 'wcsdev.mouser.lan'
	# Prod
#	WCS_PORT     = 8085
	WCS_PORT     = 8086
	WCS_ENDPOINT = 'ws/v1'

	def __init__(self, name, **init_config):
		super(WebserviceWCS, self).__init__(name, **init_config)

	@property
	def wcs_address(self):
		return '%s://%s:%s/%s' % (
			self.WCS_PROTOCOL, self.WCS_HOST, self.WCS_PORT, self.WCS_ENDPOINT
		)

	def notify_wcs_deliver(self, issue, device_id=1):
		issue_ibn    = issue['ibn']
		wcs_location = issue['chuteName']
		timestamp    = wcs_timestamp()

		payload = {
			'time':     timestamp,
			'deviceId': device_id,
			'ibn':      issue_ibn,
			'location': wcs_location,
		}

		self.logger.info('PUT to WCS for {issue_ibn} at {wcs_location} to clear from the machine - notify_wcs_issue_removed: {json}', json=system.util.jsonEncode(payload, 2))

		response = system.net.httpPut(
			url         = self.wcs_address + '/items/{issue}/deliver-notify'.format(issue=issue_ibn),
			contentType = 'application/json',
			putData     = system.util.jsonEncode(payload, 2),
		)
		self.logger.info('response from wcs is :%s' % response)
		self.log_event('wcs deliver-notify', ibn=issue_ibn, location=wcs_location)

	#-------------------Level 3 Ship WCS ------------------------------------------
	#
	# Chute name rules:
	#
	#   EuroSort divert -> REAR (dest=1)  e.g. D010011A
	#     Items always land in the rear zone physically.
	#     The routing layer passes the REAR dest_key to the sorter divert command.
	#
	#   WCS notify      -> FRONT (dest=2) e.g. D010012A
	#     WCS tracks the chute at the order/front level regardless of physical zone.
	#     All WCS payloads use the front chute name.
	#
	# Helper methods resolve the correct name from any dest_key so callers
	# never need to manually construct chute name strings.

	def _eurosort_chute_name(self, dest_key):
		"""
		Returns the EuroSort divert chute name — always REAR (dest=1).
		Items always land in the rear zone physically; EuroSort routes to dest=1.

		If dest_key is already rear (dest=1) returns its stored chuteName.
		If dest_key is front  (dest=2) returns the corresponding rear chuteName.
		Constructs the name from parts if no record is found.
		"""
		parts = str(dest_key).split('-')
		if len(parts) != 5:
			return ''
		_, station, chute, dest, side = parts

		if dest == '1':
			rec = self.destination_get(dest_key)
			if rec and rec.get('chuteName'):
				return rec['chuteName']
			return 'D%s%s1%s' % (station, chute, side)

		# Front dest_key — swap to rear
		rear_key = 'DST-%s-%s-1-%s' % (station, chute, side)
		rec = self.destination_get(rear_key)
		if rec and rec.get('chuteName'):
			return rec['chuteName']
		return 'D%s%s1%s' % (station, chute, side)

	def _wcs_chute_name(self, dest_key):
		"""
		Returns the WCS chute name — always FRONT (dest=2).
		WCS tracks the chute at the order/front level regardless of physical zone.

		If dest_key is already front (dest=2) returns its stored chuteName.
		If dest_key is rear   (dest=1) returns the corresponding front chuteName.
		Constructs the name from parts if no record is found.
		"""
		parts = str(dest_key).split('-')
		if len(parts) != 5:
			return ''
		_, station, chute, dest, side = parts

		if dest == '2':
			rec = self.destination_get(dest_key)
			if rec and rec.get('chuteName'):
				return rec['chuteName']
			return 'D%s%s2%s' % (station, chute, side)

		# Rear dest_key — swap to front
		front_key = 'DST-%s-%s-2-%s' % (station, chute, side)
		rec = self.destination_get(front_key)
		if rec and rec.get('chuteName'):
			return rec['chuteName']
		return 'D%s%s2%s' % (station, chute, side)

	def _notify_wcs_move(self, ibn, from_location, to_location, device_id=1):
		"""
		PUT /items/{ibn}/move-notify

		Base move notification. Tells WCS an item or order has moved between
		logical locations without being delivered/packed.
		Callers should use the named wrappers below.
		"""
		timestamp = wcs_timestamp()
		payload = {
			'time':         timestamp,
			'deviceId':     device_id,
			'ibn':          ibn,
			'fromLocation': from_location or '',
			'toLocation':   to_location,
		}
		self.logger.info('PUT move-notify ibn={ibn} from={frm} to={to}', ibn=ibn, frm=from_location, to=to_location)
		response = system.net.httpPut(
			url         = self.wcs_address + '/items/{ibn}/move-notify'.format(ibn=ibn),
			contentType = 'application/json',
			putData     = system.util.jsonEncode(payload, 2),
		)
		self.logger.info('move-notify response: %s' % response)
		self.log_event('wcs move-notify', ibn=ibn, from_location=from_location, to_location=to_location)

	def notify_wcs_l3ship_item_inducted(self, ibn, dest_key, device_id=1):
		"""
		Level3_Ship — item scanned and assigned to a consolidation chute.

		WCS gets move-notify with the IBN to the FRONT chute name.
		Called from _route_order, _route_high_priority, and _route_inspection
		immediately after a carrier is assigned to a destination.

		fromLocation is empty — the item has no prior WCS-tracked location
		at induction; it is entering the sorter for the first time.

		Args:
			ibn:      IBN string
			dest_key: dest_key of the assigned chute (front or rear — resolved internally)
		"""
		if bool(self._gp('squelch_wcs_updates', False)):
			return

		wcs_name = self._wcs_chute_name(dest_key)
		if not wcs_name:
			self.logger.warn('notify_wcs_l3ship_item_inducted: cannot resolve WCS chute name for %s' % dest_key)
			return

		self._notify_wcs_move(
			ibn           = ibn,
			from_location = '',
			to_location   = wcs_name,
			device_id     = device_id,
		)

	def notify_wcs_l3ship_packout_deliver(self, order_number, dest_key, device_id=1):
		"""
		Level3_Ship — order consolidated and ready for packout (NORMAL/HP chute).

		WCS gets deliver-notify with ORDER NUMBER to the FRONT chute name.
		EuroSort already has the tray at dest=1 (rear); this is purely a WCS update.
		Called from _finalize_discharge when ready_for_packout is set True.

		Args:
			order_number: order identifier string
			dest_key:     dest_key of the chute position (front or rear — resolved internally)
		"""
		if bool(self._gp('squelch_wcs_updates', False)):
			return

		wcs_name = self._wcs_chute_name(dest_key)
		if not wcs_name:
			self.logger.warn('notify_wcs_l3ship_packout_deliver: cannot resolve WCS chute name for %s' % dest_key)
			return

		self.notify_wcs_deliver(
			issue     = {'ibn': order_number, 'chuteName': wcs_name},
			device_id = device_id,
		)

	def notify_wcs_l3ship_rear_to_front(self, order_number, dest_key, device_id=1):
		"""
		Level3_Ship UC9.8 — batch door raised, order dropping rear to front.

		WCS gets move-notify with ORDER NUMBER. WCS only knows the front chute
		name so from and to are both the same front chute name — this is a state
		transition telling WCS the order is now accessible at the front zone.
		Called from _finalize_discharge when rear_drop_complete is set True.

		Args:
			order_number: order identifier string
			dest_key:     dest_key of the chute (front or rear — both resolve to front)
		"""
		if bool(self._gp('squelch_wcs_updates', False)):
			return

		wcs_name = self._wcs_chute_name(dest_key)
		if not wcs_name:
			self.logger.warn('notify_wcs_l3ship_rear_to_front: cannot resolve WCS chute name for %s' % dest_key)
			return

		# from and to are both the front chute name — WCS does not own front/rear
		self._notify_wcs_move(
			ibn           = order_number,
			from_location = wcs_name,
			to_location   = wcs_name,
			device_id     = device_id,
		)

	def notify_wcs_l3ship_ob_divert(self, ibn, from_dest_key, ob_dest_key, device_id=1):
		"""
		Level3_Ship UC3/UC4 — item diverted to an OB chute.

		WCS gets move-notify with IBN using FRONT chute names.
		OB chutes are single-level (dest=1 only) so _wcs_chute_name returns
		the front equivalent — the OB chute name is used as-is since OB
		has no front/rear distinction.
		Called from _route_ob_check when routing_to_ob_active is True.

		Args:
			ibn:           IBN string
			from_dest_key: dest_key of last known location (or None)
			ob_dest_key:   dest_key of the OB chute
		"""
		if bool(self._gp('squelch_wcs_updates', False)):
			return

		from_name = self._wcs_chute_name(from_dest_key) if from_dest_key else ''
		ob_name   = self._wcs_chute_name(ob_dest_key)

		if not ob_name:
			self.logger.warn('notify_wcs_l3ship_ob_divert: cannot resolve OB chute name for %s' % ob_dest_key)
			return

		self._notify_wcs_move(
			ibn           = ibn,
			from_location = from_name or '',
			to_location   = ob_name,
			device_id     = device_id,
		)

	def notify_wcs_l3ship_jackpot_divert(self, ibn, from_dest_key, jackpot_dest_key, device_id=1):
		"""
		Level3_Ship UC9.5 — item diverted to a JACKPOT or NOREAD lane.

		WCS gets move-notify with IBN using FRONT chute names.
		Item never consolidated to order level so IBN is the correct identifier.
		Called from _route_noread and _max_recirc.

		Args:
			ibn:               IBN string
			from_dest_key:     dest_key of last known location (or None)
			jackpot_dest_key:  dest_key of the jackpot lane
		"""
		if bool(self._gp('squelch_wcs_updates', False)):
			return

		from_name    = self._wcs_chute_name(from_dest_key) if from_dest_key else ''
		jackpot_name = self._wcs_chute_name(jackpot_dest_key)

		if not jackpot_name:
			self.logger.warn('notify_wcs_l3ship_jackpot_divert: cannot resolve jackpot chute name for %s' % jackpot_dest_key)
			return

		self._notify_wcs_move(
			ibn           = ibn,
			from_location = from_name or '',
			to_location   = jackpot_name,
			device_id     = device_id,
		)

	def notify_wcs_l3ship_bagging_move(self, order_number, from_dest_key, bagging_dest_key, device_id=1):
		"""
		Level3_Ship UC11 — completed order moved to a bagging re-induction lane.

		WCS gets move-notify with ORDER NUMBER using FRONT chute names.
		Bagging is order-level — the whole order was packed into a bag.
		Called from route_bagged_order.

		Args:
			order_number:     order identifier string
			from_dest_key:    dest_key of the consolidation chute
			bagging_dest_key: dest_key of the bagging lane
		"""
		if bool(self._gp('squelch_wcs_updates', False)):
			return

		from_name    = self._wcs_chute_name(from_dest_key) if from_dest_key else ''
		bagging_name = self._wcs_chute_name(bagging_dest_key)

		if not bagging_name:
			self.logger.warn('notify_wcs_l3ship_bagging_move: cannot resolve bagging chute name for %s' % bagging_dest_key)
			return

		self._notify_wcs_move(
			ibn           = order_number,
			from_location = from_name or '',
			to_location   = bagging_name,
			device_id     = device_id,
		)

	#-------------------Level 3 Ship WCS End --------------------------------------


class EuroSorterAccessWCS(
		WebserviceWCS,
		MongoWCS,
	):
	pass
