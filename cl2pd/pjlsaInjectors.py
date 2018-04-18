# -*- coding: utf-8 -*-
'''
PJLSA (Injector Flavour) -- A Python wrapping of Java LSA API

Based on PJLSA, originally written by:

R. De Maria     <riccardo.de.maria@cern.ch>
M. Hostettler   <michi.hostettler@cern.ch>
V. Baggiolini   <vito.baggiolini@cern.ch>


Copyright (c) CERN 2018

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.


THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Authors:

M. Carla' <michele.carla@cern.ch>
M. Beck   <mario.beck@cern.ch>
K. Li     <kevin.shing.bruce.li@cern.ch>

'''

import numpy as np

import cmmnbuild_dep_manager

# Check for LSA
mgrjars = cmmnbuild_dep_manager.Manager().jars()
lsajars=[j for j in mgrjars if 'lsa' in j ]
if len(lsajars)==0:
    raise ImportError("LSA jars not (yet) installed")

mgr   = cmmnbuild_dep_manager.Manager('pjlsa')
jpype = mgr.start_jpype_jvm()

cern   = jpype.JPackage('cern')
org    = jpype.JPackage('org')
java   = jpype.JPackage('java')
System = java.lang.System


# no java operation at import time because java might not be available and 
#  mgr.resolve will (silently) fail
if callable(org.apache.log4j.varia.NullAppender):
    null=org.apache.log4j.varia.NullAppender()
    org.apache.log4j.BasicConfigurator.configure(null)

# Java classes
ContextService    = cern.lsa.client.ContextService
ParameterService  = cern.lsa.client.ParameterService
ServiceLocator    = cern.lsa.client.ServiceLocator
SettingService    = cern.lsa.client.SettingService
TrimService       = cern.lsa.client.TrimService

BeamProcess           = cern.lsa.domain.settings.BeamProcess
ContextSettings       = cern.lsa.domain.settings.ContextSettings
ContextFamily         = cern.lsa.domain.settings.ContextFamily
Parameter             = cern.lsa.domain.settings.Parameter
ParameterSettings     = cern.lsa.domain.settings.ParameterSettings
Setting               = cern.lsa.domain.settings.Setting
FunctionSetting       = cern.lsa.domain.settings.spi.FunctionSetting
ScalarSetting         = cern.lsa.domain.settings.spi.ScalarSetting

ParametersRequestBuilder      = cern.lsa.domain.settings.factory.ParametersRequestBuilder
TrimHeadersRequestBuilder     = cern.lsa.domain.settings.TrimHeadersRequestBuilder
ContextSettingsRequestBuilder = cern.lsa.domain.settings.ContextSettingsRequestBuilder

ParameterTreesRequestBuilder       = cern.lsa.domain.settings.factory.ParameterTreesRequestBuilder
ParameterTreesRequest              = cern.lsa.domain.settings.ParameterTreesRequest
ParameterTreesRequestTreeDirection = jpype.JClass('cern.lsa.domain.settings.ParameterTreesRequest$TreeDirection')

LHC  = cern.accsoft.commons.domain.CernAccelerator.LHC
PS   = cern.accsoft.commons.domain.CernAccelerator.PS
SPS  = cern.accsoft.commons.domain.CernAccelerator.SPS
LEIR = cern.accsoft.commons.domain.CernAccelerator.LEIR
PSB  = cern.accsoft.commons.domain.CernAccelerator.PSB

ContextFamilies = {'beamprocess' : ContextFamily.BEAMPROCESS,
                   'cycle'       : ContextFamily.CYCLE,
                   'supercycle'  : ContextFamily.SUPERCYCLE}

Accelerators={'lhc':  LHC, 'ps':   PS, 'sps':  SPS,
              'lear': LEIR, 'psb':  PSB, }


class LSAClient(object):
    def __init__(self, accelerator, server='gpn'):
        System.setProperty("lsa.server", server)
        self.accelerator = Accelerators.get(accelerator)

        self._contextService   = ServiceLocator.getService(ContextService)
        self._trimService      = ServiceLocator.getService(TrimService)
        self._settingService   = ServiceLocator.getService(SettingService)
        self._parameterService = ServiceLocator.getService(ParameterService)
        self._settingService   = ServiceLocator.getService(SettingService)

    def findParameterGroups(self):
        find = self._parameterService.findParameterGroupsByAccelerator
        groups = [grp.getName() for grp in find(self.accelerator)]
        return sorted([str(grp) for grp in groups])

    def findParameterNames(self, device=None, group=None):
        req = ParametersRequestBuilder()
        if device is not None:
            req.setDeviceName(device)
        if group is not None:
            req.setParameterGroup(group)
        lst = self._parameterService.findParameters(req.build())
        return sorted([pp.getName() for pp in lst])

    def findUsers(self):
        users = self._contextService.findUsers(self.accelerator)
        return sorted([str(u) for u in users])

    def findUserContextMappingHistory(self, start, end):
        contextFamily = ContextFamilies.get('cycle')
        t0 = java.sql.Timestamp.valueOf(start).fastTime
        t1 = java.sql.Timestamp.valueOf(end).fastTime
        res = self._contextService.findUserContextMappingHistory(self.accelerator,
                                                                 contextFamily, t0, t1)
        out=[ (ct.mappingTimestamp/1000.,
               ct.contextName,
               ct.user) for ct in res]
        return out

    def findLastCycleMapping(self, name, time):
        contextFamily = ContextFamilies.get('cycle')
        t = java.sql.Timestamp.valueOf(time).fastTime
        res = self._contextService.findUserContextMappingHistory(self.accelerator,
                                                                 contextFamily, 0, t)
        ts = 0
        cycle = None
        for ct in res:
                if ct.user == name:
                    if ct.mappingTimestamp > ts:
                        ts = ct.mappingTimestamp
                        cycle = ct.contextName

        return cycle, ts

    def _findParameters(self, name, group):
        req = ParametersRequestBuilder()
        req.setAccelerator(self.accelerator)
        req.setParameterName(name)
        req.setParameterGroup(group)
        return self._parameterService.findParameters(req.build())
    
    def _findParameters2(self, name):
        req = ParametersRequestBuilder()
        req.setAccelerator(self.accelerator)
        req.setParameterName(name)
        return self._parameterService.findParameters(req.build())

    def _parseSetting(self, setting, part):
        time = None
        value = None
        if type(setting) is ScalarSetting:
            if part == 'value':
                value = setting.getScalarValue().getDouble()
            elif part == 'target':
                value = setting.getTargetScalarValue().getDouble()
            elif part == 'correction':
                value = setting.getCorrectionScalarValue().getDouble()
            else:
                raise ValueError('Invalid Setting Part: ' + part)
        elif type(setting) is FunctionSetting:
            if part == 'value':
                df = setting.getFunctionValue()
            elif part == 'target':
                df = setting.getTargetFunctionValue()
            elif part == 'correction':
                df = setting.getCorrectionFunctionValue()
            else:
                raise ValueError('Invalid Setting Part: ' + part)
            value = [df.toYArray()[:], df.toXArray()[:]]

        return value

    def getTrim(self, cycle, name, group, time, part='value'):
        context = self._contextService.findStandAloneCycle(cycle)
        processes = context.getBeamProcesses()
        parameter = self._findParameters(name, group)
        if len(parameter) == 0:
            return None
        time = java.sql.Timestamp.valueOf(time)
        context_setting = self._settingService.findContextSettings(context, parameter, time)
        parameter_setting = context_setting.getParameterSettings(list(parameter)[0])
        if parameter_setting is None:
            return None

        time = []
        value = []
        for bp in list(processes):
            setting = parameter_setting.getSetting(bp)
            aux = self._parseSetting(setting, part)
            if aux is None:
                continue

            value.append(aux[0])
            time.append(aux[1] + bp.getStartTime())

        value = np.concatenate(value)
        time = np.concatenate(time)

        return {'time' : time, 'value': value}


    def _getRawTrimHeaders(self, processes, parameters, start, end):
        trimhReq = TrimHeadersRequestBuilder()
        trimhReq.beamProcesses(processes)
        trimhReq.parameters(parameters)
        trimHeaders = trimhReq.build()
        rawHeaders = self._trimService.findTrimHeaders(trimHeaders)

        ts = java.sql.Timestamp.valueOf(start)
        rawHeaders = [th for th in rawHeaders if not th.createdDate.before(ts)]

        ts = java.sql.Timestamp.valueOf(end)
        rawHeaders = [th for th in rawHeaders if not th.createdDate.after(ts)]
        return rawHeaders

    def getTrims(self, cycle, name, group, start, end, part='value'):
        context = self._contextService.findStandAloneCycle(cycle)
        processes = context.getBeamProcesses()

        paramReq = ParametersRequestBuilder()
        paramReq.setAccelerator(self.accelerator)
        paramReq.setParameterName(name)
        paramReq.setParameterGroup(group)
        parameters = self._parameterService.findParameters(paramReq.build())

        timestamps = []
        value = []
        for th in self._getRawTrimHeaders(processes, parameters, start, end):
            csrb = ContextSettingsRequestBuilder()
            csrb.standAloneContext(context)
            csrb.parameters(parameters)
            csrb.at(th.createdDate.toInstant())
            contextSettings =  self._settingService.findContextSettings(csrb.build())

            for pp in parameters:
                parameterSetting = contextSettings.getParameterSettings(pp)
                if parameterSetting is None:
                    continue

                setting = None
                time_aux = []
                value_aux = []
                for bp in list(processes):
                    setting = parameterSetting.getSetting(bp)
                    aux = self._parseSetting(setting, part)
                    if aux is None:
                        continue

                    value_aux.append(aux[0])
                    time_aux.append(aux[1] + bp.getStartTime())

            value_aux = np.concatenate(value_aux)
            time_aux = np.concatenate(time_aux)
            timestamps.append(th.createdDate.getTime()/1000)
            value.append({'time' : time_aux,'value': value_aux})
        return timestamps, value
    
    def getTrims2(self, cycle, name, start, end, part='value'):
        context = self._contextService.findStandAloneCycle(cycle)
        processes = context.getBeamProcesses()

        paramReq = ParametersRequestBuilder()
        paramReq.setAccelerator(self.accelerator)
        paramReq.setParameterName(name)
        #paramReq.setParameterGroup(group)
        parameters = self._parameterService.findParameters(paramReq.build())

        timestamps = []
        value = []
        comment=[]
        for th in self._getRawTrimHeaders(processes, parameters, start, end):
            csrb = ContextSettingsRequestBuilder()
            csrb.standAloneContext(context)
            csrb.parameters(parameters)
            csrb.at(th.createdDate.toInstant())
            mycsrb=csrb.build()
            contextSettings =  self._settingService.findContextSettings(mycsrb)

            for pp in parameters:
                parameterSetting = contextSettings.getParameterSettings(pp)
                if parameterSetting is None:
                    continue

                setting = None
                time_aux = []
                value_aux = []
                for bp in list(processes):
                    setting = parameterSetting.getSetting(bp)
                    aux = self._parseSetting(setting, part)
                    if aux is None:
                        continue

                    value_aux.append(aux[0])
                    time_aux.append(aux[1] + bp.getStartTime())
    

            value_aux = np.concatenate(value_aux)
            time_aux = np.concatenate(time_aux)
            timestamps.append(th.createdDate.getTime()/1000)
            comment.append(th.description)
            value.append({'time' : time_aux,'value': value_aux})
        return timestamps, value, comment
