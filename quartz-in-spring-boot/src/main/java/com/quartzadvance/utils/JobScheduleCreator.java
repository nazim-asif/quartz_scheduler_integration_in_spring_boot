package com.quartzadvance.utils;

import com.quartzadvance.entity.SchedulerJobInfo;
import lombok.extern.slf4j.Slf4j;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.SimpleTrigger;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.scheduling.quartz.SimpleTriggerFactoryBean;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.util.Date;

/**
 * @author Nazim Uddin Asif
 * @version 0.1
 */
@Slf4j
@Component
public class JobScheduleCreator {

    /**
     * Create Quartz Job.
     *
     * @param jobInfo it represents Job scheduling information.
     * @return JobDetail object
     */
    public JobDetail createJob(SchedulerJobInfo jobInfo) throws ClassNotFoundException {
        JobDetailFactoryBean factoryBean = new JobDetailFactoryBean();
        factoryBean.setJobClass((Class<? extends QuartzJobBean>) Class.forName(jobInfo.getJobClass()));
        factoryBean.setDurability(jobInfo.getIsDurable());
        factoryBean.setName(jobInfo.getJobName());
        factoryBean.setGroup(jobInfo.getJobGroup());
        // set job data map
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(jobInfo.getJobName(), jobInfo);
        factoryBean.setJobDataMap(jobDataMap);

        factoryBean.afterPropertiesSet();
        return factoryBean.getObject();
    }

    /**
     * Create cron trigger.
     *
     * @param info it represents Job scheduling information.
     * @return {@link CronTrigger}
     */
    public CronTrigger createCronTrigger(SchedulerJobInfo info) {
        CronTriggerFactoryBean factoryBean = new CronTriggerFactoryBean();
        factoryBean.setName(info.getJobName());
        factoryBean.setStartTime(new Date(System.currentTimeMillis() + info.getInitialOffsetMs()));
        factoryBean.setCronExpression(info.getCronExpression());
        factoryBean.setMisfireInstruction(info.getMisFireInstruction());
        factoryBean.setDescription(info.getDescription());
        try {
            factoryBean.afterPropertiesSet();
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
        }
        return factoryBean.getObject();
    }

    /**
     * Create simple trigger.
     *
     * @param info it represents Job scheduling information.
     * @return {@link SimpleTrigger}
     */
    public SimpleTrigger createSimpleTrigger(SchedulerJobInfo info) {
        SimpleTriggerFactoryBean factoryBean = new SimpleTriggerFactoryBean();
        factoryBean.setName(info.getJobName());
        factoryBean.setStartTime(new Date(System.currentTimeMillis()));
        factoryBean.setRepeatInterval(info.getRepeatTime());
        factoryBean.setRepeatCount(info.getRunForever() ? SimpleTrigger.REPEAT_INDEFINITELY : info.getTotalFireCount() - 1);
        factoryBean.setMisfireInstruction(info.getMisFireInstruction());
        factoryBean.setDescription(info.getDescription());
        factoryBean.afterPropertiesSet();
        return factoryBean.getObject();
    }
}
