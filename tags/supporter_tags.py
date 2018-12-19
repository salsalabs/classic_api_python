import csv
import os
import logging
import os.path
import pathlib
import threading

from base import SaverBase

class SupporterTagReader (threading.Thread):
    """
    Read supporter records at a nominal rate of 500 per chunk. Output them
    singly to the output queues.
    """

    def __init__(self, **kwargs):
        """
        Initialize a SupporterTagReader object
        
        Params in kwargs:
        
        :threadID:          numeric cardinal thread identifier
        :cred:              login credentials (from the YAML file)
        :session:           requests object to read from Salsa
        :cond:              conditions to use to filter supporters
        :supporterSaveQueue: supporter save queue
        :start:             starting offset in the Salsa database
        :exitFlag:          boolean indicating when processing should stop
        """

        threading.Thread.__init__(self)
        self.__dict__.update(kwargs)
        self.threadName = type(self).__name__
        
        logName = f"{self.threadName}_{self.threadID:02d}"
        self.log = logging.getLogger(logName)
        console = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s: %(name)-18s %(levelname)-8s %(message)s')
        console.setFormatter(formatter)
        console.setLevel(logging.DEBUG)
        self.log.addHandler(console)

    def run(self):
        """
        Run the thread.  Overrides Threading.run()
        """

        self.log.info('starting')
        self.process_data()
        self.log.info("Ending  " + self.threadName)

    def process_data(self):
        """
        Read supporters from the database.  Queue them up individually
        to the output queue(s).
        """

        offset = self.offset
        count = 500
        conditions = [
            "tag_data.database_table_KEY=142",
            "tag.tag_KEY IS NOT EMPTY",
            "tag.tag is NOT EMPTY"
        ]
        tables = "supporter(tag_data.table_KEY=supporter_KEY)tag_data(tag_KEY)tag"
        includes = "supporter_KEY,Email,tag.prefix,tag.tag,tag.tag_KEY"
        while count == 500:
            payload = {'json': True,
                       "limit": f"{offset},{count}",
                       'object': tables,
                       'condition': "&condition=".join(conditions),
                       'include': includes}
            u = f"https://{self.cred['host']}/api/getLeftJoin.sjs"
            self.log.info(f"reading {count} from {offset:7d}")
            r = self.session.get(u, params=payload)
            j = r.json()

            # Iterate through the records and push each onto the output queues.
            useful = lambda x: x["tag"] and len(x["tag"]) > 0 and x["tag_KEY"] not in ExcludedTagKeys
            for supporter in filter(useful, j):
                self.supporterSaveQueue.put(supporter)

            count = len(j)
            offset += count

class SupporterTagSaver (SaverBase):
    """
    Accepts Group records from a queue, then write them to a CSV file.
    """

    def __init__(self, **kwargs):
        """
        Initialize a GroupSaver instance
        """
        SaverBase.__init__(self, **kwargs)

    def getFieldMap(self):
        """
        Specify the output fields for the CSV file.  Overrides SaverBase.getFieldMap();
        """
        return SupporterTagMap

    def process_data(self):
        """
        Accept a supporter from the supporter save queue and save it to a CSV
        file.
        """

        count = self.maxRecs
        while not self.exitFlag:
            supporter = self.supporterSaveQueue.get()
            if not supporter:
                continue
            if count >= self.maxRecs:
                count = 0
                self.openFile()
            # csv writer complains if there's stuff in the record
            # that's not going to be written
            if 'object' in supporter:
                del supporter['object']
            if 'key' in supporter:
                del supporter['key']

            # Create a new dict of output headers and Classic values.
            m = {}
            for k in SupportMapOrder:
                v = self.getFieldMap()[k]
                if v:
                    m[k] = str.strip(supporter[v])
            try:
                self.writer.writerow(m)
                count = count + 1
            except UnicodeEncodeError:
                self.log.error(f"{self.threadName}_{self.threadID:02d}: UnicodeEncodeError on {supporter}")

SupporterTagMap = {
    "supporter_KEY": "supporter_KEY",
    "Email": "Email",
    "Prefix": "prefix",
    "Tag": "tag",
    "TagKey": "tag_KEY"}
SupportMapOrder = [
    "supporter_KEY",
    "Email",
    "Prefix",
    "Tag",
    "TagKey" ]
ExcludedTagKeys = [
    205842, 205845, 205850, 207864, 208431, 208726, 209040, 209575,
    209700, 210530, 210591, 210894, 211297, 211480, 213572, 213589,
    214297, 214393, 214538, 214672, 215484, 216035, 216275, 216278,
    216846, 216876, 217121, 217130, 217345, 217525, 218058, 218311,
    218478, 219376, 219640, 219684, 219785, 220185, 220675, 220748,
    221056, 221430, 221464, 222090, 222277, 222601, 222665, 222843,
    222969, 222985, 223009, 223346, 224046, 224147, 224737, 225119,
    225244, 226094, 228292, 228355, 230047, 233071, 233083, 233767,
    237163, 237703, 239392, 239532, 240435, 240759, 241022, 241157,
    242030, 242063, 242729, 243395, 243581, 245289, 245424, 245743,
    245745, 245751, 246045, 246054, 246304, 246315, 247253, 247255,
    247489, 248348, 248514, 250919, 251103, 251104, 251223, 251620,
    251688, 251991, 252953, 253067, 253465, 253901, 254215, 254416,
    254418, 254980, 255814, 255818, 256254, 256579, 258364, 258436,
    258791, 260650, 260850, 261338, 263026, 263578, 265492, 265494,
    265496, 265497, 265498, 265499, 266104, 267257, 268177, 268186,
    268193, 269583, 269586, 269587, 269601, 270547, 270885, 271681,
    271692, 272131, 272288, 272353, 272463, 272647, 273268, 273582,
    273710, 274528, 274768, 275113, 275147, 276065, 276563, 277083,
    277418, 277443, 277567, 278079, 278507, 278654, 279178, 279402,
    279734, 279944, 280044, 280070, 280165, 280427, 280812, 280867,
    281364, 281448, 282034, 282262, 282272, 282442, 284530, 285106,
    285312, 285407, 285624, 285771, 286057, 286085, 286328, 286354,
    286452, 286508, 286522, 286759, 286784, 286886, 287005, 287137,
    287435, 287436, 287437, 287438, 287439, 287440, 287441, 288166,
    288359, 289307, 289437, 289873, 289920, 290147, 291150, 291228,
    291288, 291555, 291637, 292227, 292846, 293255, 293423, 293493,
    293675, 294045, 294309, 295194, 295499, 295761, 296125, 296290,
    296415, 296819, 296935, 297038, 297260, 297358, 297467, 297478,
    297722, 297983, 298928, 298944, 299431, 300057, 300360, 300463,
    300521, 300522, 300523, 300524, 300525, 300526, 300527, 300528,
    300604, 300760, 300944, 301251, 301526, 301820, 301821, 301822,
    301891, 303827, 304847, 305435, 305770, 306172, 306331, 306756,
    306794, 306970, 306971, 306972, 306973, 306974, 306975, 306976,
    306977, 306978, 307137, 308905, 308906, 308909, 308910, 308933,
    308940, 308941, 309199, 309200, 309913, 310331, 310332, 310341,
    310342, 310343, 310588, 310589, 310813, 310814, 311070, 311131,
    311132, 311592, 311593, 311595, 311596, 311600, 311601, 312593,
    312594, 312609, 312610, 312672, 312801, 312802, 312805, 312814,
    312816, 312874, 313013, 313016, 313017, 313214, 313216, 313217,
    313220, 313221, 313225, 313226, 313229, 313230, 313259, 313292,
    313319, 313559, 313560, 313658, 313802, 313803, 313812, 313882,
    313934, 314463, 314464, 314466, 314467, 314468, 314469, 314552,
    314553, 314867, 314875, 314876, 314877, 314878, 314879, 314980,
    314981, 315301, 315302, 315403, 315450, 315451, 315453, 315454,
    315557, 315558, 315850, 315851, 316003, 316004, 316338, 316339,
    316341, 316342, 316343, 316344, 316347, 316348, 316455, 317015,
    317059, 317075, 317136, 317137, 317402, 317403, 317404, 317405,
    317406, 317407, 317408, 317409, 317410, 317411, 317412, 317413,
    317414, 317415, 317416, 317417, 317418, 317419, 317421, 317422,
    317426, 317427, 317432, 317433, 317437, 317438, 317439, 317440,
    317830, 317866, 317995, 318028, 318031, 318033, 318034, 318099,
    318305, 318728, 319401, 319505, 319826, 319828, 319865, 319995,
    320073, 320583, 320653, 320654, 320655, 321184, 321201, 321202,
    321203, 321294, 321369, 321436, 321437, 321472, 321473, 321474,
    321475, 321571, 321572, 321575, 321576, 321739, 321740, 322143,
    322144, 322417, 322454, 322551, 322876, 322913, 323089, 323162,
    323163, 323165, 323167, 323168, 323169, 323170, 323217, 323255,
    323305, 323306, 323445, 323449, 323450, 323451, 323452, 323965,
    324141, 324153, 324242, 324243, 324513, 324514, 324519, 324524,
    324525, 324526, 324613, 324614, 324617, 324618, 324622, 325173,
    325174, 325175, 325176, 325433, 325492, 325545, 325931, 325932,
    326163, 326214, 326364, 326737, 327175, 327177, 327178, 327214,
    327215, 327216, 327217, 327218, 327219, 327220, 327221, 327222,
    327223, 327224, 327225, 327226, 327311, 327382, 327780, 327796,
    327797, 327812, 327813, 327920, 328190, 328513, 328514, 328538,
    328539, 328659, 328676, 328734, 328735, 328893, 328926, 328927,
    329053, 329094, 329095, 329216, 329235, 329260, 329363, 329364,
    329499, 329500, 329518, 329519, 329520, 329538, 329611, 329612,
    329653, 329654, 329747, 329755, 329756, 329812, 329813, 329876,
    330168, 330236, 330237, 330380, 330381, 330431, 330432, 330435,
    330436, 330438, 330480, 330481, 330482, 330483, 330484, 330486,
    330487, 330519, 330520, 330920, 330921, 330963, 331136, 331250,
    331367, 331368, 331381, 331597, 331652, 331653, 331654, 331655,
    331727, 331728, 331851, 331854, 331855, 331856, 331857, 332026,
    332027, 332053, 332054, 332060, 332061, 332369, 332370, 332399,
    332400, 332537, 332611, 332893, 332894, 332997, 332998, 332999,
    333000, 333037, 333049, 333050, 333051, 333052, 333053, 333054,
    333055, 333108, 333111, 333118, 333119, 333285, 333286, 333287,
    333462, 333463]
