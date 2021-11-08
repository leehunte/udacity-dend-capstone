import boto3
import math
import logging
import pandas as pd
import xml.etree.ElementTree as et

class ServiceRequestAppraiser:
    matrix = None

    def __EvaluateCablingIC(self, details, price):
        estimate = 0
        qty = int(details["quantity"])
        locs = int(details["eligible_entities"])
        uprice = float(price.find("unit_price").text)
        iprice = float(price.find("installation_price").text)

        rolls = 0
        if qty == 0:
            rolls = math.ceil((300 * locs) / 1000)
        elif qty <= 300:
            rolls = math.ceil((qty * 300 * locs) / 1000)
        elif qty <= 1000 * locs:
            rolls = math.ceil((qty * locs) / 1000)
        else:
            rolls = math.ceil(qty / 1000)
        estimate = rolls * uprice

        if details["needs_installation"] == True:
            estimate += rolls * iprice

        return estimate

    def __EvaluateCablingBM(self, details, price):
        estimate = 0
        qty = int(details["quantity"])
        locs = int(details["eligible_entities"])
        mprice = float(price.find("support_price").text)

        rolls = 0
        if qty <= 300:
            rolls = math.ceil((qty * 300 * locs) / 1000)
        elif qty <= 1000 * locs:
            rolls = math.ceil((qty * locs) / 1000)
        else:
            rolls = math.ceil(qty / 1000)
        estimate = rolls * mprice

        return estimate

    def __EvaluateRacksIC(self, details, price):
        estimate = 0
        qty = int(details["quantity"])
        locs = int(details["eligible_entities"])
        uprice = float(price.find("unit_price").text)
        iprice = float(price.find("installation_price").text)

        if qty <= locs:
            estimate = locs * uprice
        else:
            estimate = qty * uprice

        if details["needs_installation"] == True:
            estimate += locs * iprice

        return estimate

    def __EvaluateRacksBM(self, details, price):
        locs = int(details["eligible_entities"])
        mprice = float(price.find("support_price").text)

        return locs * mprice

    def __EvaluateSwitchesIC(self, details, price):
        estimate = 0
        qty = int(details["quantity"])
        locs = int(details["eligible_entities"])
        uprice = float(price.find("unit_price").text)
        iprice = float(price.find("installation_price").text)

        each = 0
        if qty <= locs:
            each = locs
        elif locs * 48 < qty:
            each = math.ceil(qty / 45)
        else:
            each = qty
        estimate = each * uprice

        if details["needs_installation"] == True:
            estimate += each * iprice

        return estimate

    def __EvaluateSwitchesBM(self, details, price):
        estimate = 0
        qty = int(details["quantity"])
        locs = int(details["eligible_entities"])
        mprice = float(price.find("support_price").text)

        if qty <= locs:
            estimate = locs * mprice
        elif locs * 48 < qty:
            estimate = math.ceil(qty / 45) * mprice
        else:
            estimate = qty * mprice

        return estimate

    def __EvaluateWapIC(self, details, price):
        estimate = 0
        qty = int(details["quantity"])
        locs = int(details["eligible_entities"])
        uprice = float(price.find("unit_price").text)
        iprice = float(price.find("installation_price").text)

        each = 0
        if qty <= locs * 36:
            each = qty
        else:
            each = locs * 36
        estimate = each * uprice

        if details["needs_installation"] == True:
            estimate += each * iprice

        return estimate

    def __EvaluateWapBM(self, details, price):
        estimate = 0
        qty = int(details["quantity"])
        locs = int(details["eligible_entities"])
        mprice = float(price.find("support_price").text)

        each = 0
        if qty <= locs * 36:
            each = qty
        else:
            each = locs * 36

        return each * mprice

    def __EvaluateControllerIC(self, details, price):
        estimate = 0
        qty = int(details["quantity"])
        locs = int(details["eligible_entities"])
        uprice = float(price.find("unit_price").text)
        iprice = float(price.find("installation_price").text)

        each = 0
        if qty < locs:
            each = qty
        else:
            each = locs
        estimate = each * uprice

        if details["needs_installation"] == True:
            estimate += each * iprice

        return estimate

    def __EvaluateControllerBM(self, details, price):
        estimate = 0
        qty = int(details["quantity"])
        locs = int(details["eligible_entities"])
        mprice = float(price.find("support_price").text)

        each = 0
        if qty < locs:
            each = qty
        else:
            each = locs

        return each * mprice

    def __init__(self):
        self.matrix = pd.DataFrame({
            "Cabling":[self.__EvaluateCablingIC, self.__EvaluateCablingBM],
            "Racks":[self.__EvaluateRacksIC, self.__EvaluateRacksBM],
            "Switches":[self.__EvaluateSwitchesIC, self.__EvaluateSwitchesBM],
            "Wap":[self.__EvaluateWapIC, self.__EvaluateWapBM],
            "Wireless Controller":[self.__EvaluateControllerIC, self.__EvaluateControllerBM]},
            ["Internal Connections","Basic Maintenance Of Internal Connections"]
        )
