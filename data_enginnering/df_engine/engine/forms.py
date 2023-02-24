"""
Module Name: forms.py
Install Date : 07-AUG-2020 (Initial)
Modified Date : []
Functionality : Core logic to compute the directions to fire inorder to hit the target,
                alone with the angles in degrees
Version Notes:
07-AUG-2020     -   Initial Push to Production
"""
# Unwanted pylint checks are defined here
# pylint: disable=W1203, W0703, C0301, R0914, R0902

from math import sqrt, atan2, ceil, degrees
from datetime import datetime as dt
from copy import deepcopy
import logging
import json
import os
from flask.logging import default_handler

FORMAT = "%(levelname)s %(asctime)s - %(message)s -->%(lineno)d |%(module)s "
logging.basicConfig(
    filename="monitor.txt",
    level=logging.DEBUG,
    format=FORMAT,
    filemode='a+'
)
l = logging.getLogger()
l.addHandler(default_handler)


class Compute:
    """Base class for the core logic, Can be imported to a framework or can be executed as it is,
    provided necessary arguments are supplied during instantiation you need to supply the creds
    post which validate and compute functions can be invoked"""
    def __init__(self, req):
        l.info(f"Request : {req}")
        self.player = str(req.get('ply'))
        self.dimensions = [int(i) for i in req.get('dim').split(',')]
        self.player_pos = [int(i) for i in req.get('pp').split(',')]
        self.target_pos = [int(i) for i in req.get('tp').split(',')]
        self.distance = int(req.get('dist'))

        self.room_x = self.dimensions[0]
        self.room_y = self.dimensions[1]
        self.player_x = self.player_pos[0]
        self.player_y = self.player_pos[1]
        self.guard_x = self.target_pos[0]
        self.guard_y = self.target_pos[1]
        self.max_distance = self.distance

        self.max_x = self.player_x + self.distance + 1
        self.max_y = self.player_y + self.distance + 1
        self.out = None
        self.full_radius = 360
        self.config_dir = (os.path.abspath(os.path.join(os.getcwd())))
        self.config_file = '/config/restrictions.json'

    def getconfig(self, arg):
        """ Functionality to consider the restrictions"""
        with open(self.config_dir+self.config_file) as json_fp:
            cond = json.load(json_fp)
        return cond[arg]

    def validate(self):
        """ Mandatory Validations are performed here """

        if not 1 < self.room_x <= self.getconfig('room_x_max'):
            self.out = f"dimension (x) of the room should be <= {self.getconfig('room_x_max')} but received {self.room_x})"
        if not 1 < self.room_y <= self.getconfig('room_y_max'):
            self.out = f"dimension (y) of the room should be <= {self.getconfig('room_y_max')} but received {self.room_x})"
        if ((self.player_x == self.guard_x) and (self.player_y == self.guard_y)):
            self.out = f"player and target shouldn't be sharing same position{self.player_x,self.guard_x,self.player_y,self.guard_y}"
        if not 0 < self.player_x <= self.room_x or not 0 < self.player_y <= self.room_y:
            self.out = f"player is positioned outside the room {self.player_x,self.player_y} dim {self.room_x,self.room_y}"
        if not 0 < self.guard_x <= self.room_x or not 0 < self.guard_y <= self.room_y:
            self.out = f"target is positioned outside the room {self.guard_x,self.guard_y} dim {self.room_x,self.room_y}"
        if not 1 < self.max_distance <= 10000:
            self.out = f"distance is limited to range of  1-10000 but received {self.max_distance}"

        if self.out is None:
            return True, self.out
        l.critical(f"Validation Error : {self.out}")
        return False, self.out

    def get_dist(self, point_x, point_y):
        """Gets distance between player and a point"""
        dist = sqrt((point_x - self.player_x) ** 2 + (point_y -
                                                      self.player_y) ** 2)
        return dist

    def get_angle(self, point_x, point_y):
        """Gets angle between player and a point in RAD"""
        angle = atan2(point_y - self.player_y, point_x - self.player_x)
        # print(f"point_x {point_x} point_y {point_x} angle {angle}")
        return angle

    def get_first_quadrant(self):
        """gets the number of copies that need to be done along the axis
        and gets all the guard and player coords"""
        num_copies_x = ceil(self.max_x / self.room_x)
        num_copies_x = int(num_copies_x)
        num_copies_y = ceil(self.max_y / self.room_y)
        num_copies_y = int(num_copies_y)

        player_exp_x = []
        player_exp_y = []
        guard_exp_x = []
        guard_exp_y = []
        # Loop expands along the x axis
        for i in range(0, num_copies_x + 1, 1):
            temp_player_y_list = []
            temp_guard_y_list = []
            r_x = self.room_x * i

            if len(player_exp_x) == 0:
                clone_player_x = self.player_x
            else:
                clone_player_x = (r_x - player_exp_x[-1][0]) + r_x
            player_exp_x.append([clone_player_x, self.player_y, 1])

            if len(guard_exp_x) == 0:
                clone_target_x = self.guard_x
            else:
                clone_target_x = (r_x - guard_exp_x[-1][0]) + r_x
            guard_exp_x.append([clone_target_x, self.guard_y, 7])

            # Loop expands along the x axis
            for j in range(1, num_copies_y + 1, 1):
                r_y = self.room_y * j
                if len(temp_guard_y_list) == 0:
                    clone_target_y = (r_y - self.guard_y) + r_y
                    temp_guard_y_list.append(clone_target_y)
                else:
                    clone_target_y = (r_y - temp_guard_y_list[-1]) + r_y
                    temp_guard_y_list.append(clone_target_y)
                guard_exp_y.append([clone_target_x, clone_target_y, 7])

                if len(temp_player_y_list) == 0:
                    clone_player_y = (r_y - self.player_y) + r_y
                    temp_player_y_list.append(clone_player_y)
                else:
                    clone_player_y = (r_y - temp_player_y_list[-1]) + r_y
                    temp_player_y_list.append(clone_player_y)
                player_exp_y.append([clone_player_x, clone_player_y, 1])

        return player_exp_x + guard_exp_x + player_exp_y + guard_exp_y

    def other_quadrants(self, matrix):
        """Uses the mirror_pos from the first quadrant and flips its to the other
        3 quadrants"""
        quad_2 = deepcopy(matrix)
        quad_2t = [-1, 1]
        quad_2f = []
        for j in range(len(quad_2)):
            mirror_pos = [quad_2[j][i] * quad_2t[i] for i in range(2)]
            dist = self.get_dist(mirror_pos[0], mirror_pos[1])

            if dist <= self.max_distance:
                mirror_pos.append(matrix[j][2])
                quad_2f.append(mirror_pos)

        quad_3 = deepcopy(matrix)
        quad_3t = [-1, -1]
        quad_3f = []
        for j in range(len(quad_3)):
            mirror_pos = [quad_3[j][i] * quad_3t[i] for i in range(2)]
            dist = self.get_dist(mirror_pos[0], mirror_pos[1])

            if dist <= self.max_distance:
                mirror_pos.append(matrix[j][2])
                quad_3f.append(mirror_pos)

        quad_4 = deepcopy(matrix)
        quad_4t = [1, -1]
        quad_4f = []
        for j in range(len(quad_4)):
            mirror_pos = [quad_4[j][i] * quad_4t[i] for i in range(2)]
            dist = self.get_dist(mirror_pos[0], mirror_pos[1])

            if dist <= self.max_distance:
                mirror_pos.append(matrix[j][2])
                quad_4f.append(mirror_pos)

        return quad_2f, quad_3f, quad_4f

    def filter_target_hit(self, matrix):
        """Uses a dict with angles as key
        Filters by range and by distance of the same angle (closer always
        wins)"""
        target = {}
        # for i in range(len(matrix)):
        for i, j in list(enumerate(matrix)):
            dist = self.get_dist(matrix[i][0], matrix[i][1])
            angle = self.get_angle(matrix[i][0], matrix[i][1])
            test_a = self.max_distance >= dist > 0
            test_b = angle not in target
            test_c = angle in target and dist < target[angle][1]
            if test_a and (test_b or test_c):
                target[(angle)] = [matrix[i], dist]
        return target

    def calculate(self):
        """
         Makes a room instance with all the parameters given, Generates all possible points
         in the first quadrant and use that to fetch positions in all  other quadrants, filters
         the Original player, and all mirrored targed postion and also return o/p in degrees"""

        start_time = dt.utcnow()
        try:
            quad_1 = self.get_first_quadrant()
            quad_2, quad_3, quad_4 = self.other_quadrants(quad_1)
            final_list = quad_1 + quad_2 + quad_3 + quad_4
            final_dict = self.filter_target_hit(final_list)

            rads = []
            final_angles = []

            for key, val in final_dict.items():
                if int(val[0][2]) == 7:
                    if (float(key)) < 0:
                        rads.append(float(key))
                    else:
                        rads.append(float(key))

            degrees_list = [degrees(r) for r in rads]
            final_angles = [float(self.full_radius)+degree if degree < 0 else degree for degree in degrees_list]

        except Exception as error:
            l.critical(str(error))
            return str(error)

        end_time = dt.utcnow()
        time_taken = str(end_time - start_time)

        resp = {
            'player': self.player,
            'no_of_direction': len(final_angles),
            'angles': final_angles,
            'time taken': time_taken
        }

        l.info(f"Response : {resp}")
        return resp
