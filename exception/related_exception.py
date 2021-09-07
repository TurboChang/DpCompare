#!/usr/bin/env python
# encoding: utf-8
# author TurboChang

class RegressionException(Exception):
    """异常基类"""

    def __init__(self, msg='', logger=None):
        self.message = msg
        if logger:
            logger.error(msg)

        Exception.__init__(self, msg)

    def __repr__(self):
        return self.message

    __str__ = __repr__

class EmailException(RegressionException):
    """抛出邮件模块异常"""
    pass

class W_Report_Exception(RegressionException):
    """抛出报告写入异常"""
    pass