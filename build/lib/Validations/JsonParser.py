# Databricks notebook source
import json

def GetRules(ruleFilePath):
    with open(ruleFilePath) as rf:
        rules = json.load(rf)
        return rules
        
# rules = GetRules("C:/rulengine_master/rule_file.json")
# print(rules)

def GetElementByKeyValue(rules, rKey, rValue):
    for dic in rules:
        for key in dic:
            if key == rKey and dic[key] == rValue:
                return dic

# print(GetElementByKeyValue(rules,"RuleID","1"))


def GetElementByKey(rules, rKey):
    print("-----")
    print(rKey)
    # print(rules)
    for dic in rules:
        # print(dic)
        for key in dic:
            # print(key + "----" + rKey)
            if key == rKey:
                return dic

# print(GetElementByKey(rules,"RuleID"))

def GetAllElementByKeyValue(rules, rKey, rValue):
    elementList = []
    for dic in rules:
        for key in dic:
            if key == rKey and dic[key] == rValue:
                elementList.append(dic)
    return elementList


def GetAllValueByKey(rules, rKey):
    elementList = []
    for dic in rules:
        for key in dic:
            if key == rKey:
                elementList.append(dic[key])
    return elementList


def GetValueByKey(rules, rKey):
    elementList = []
    for dic in rules:
        for key in dic:
            if key == rKey:
                elementList.append(dic[key])
    return elementList


