package org.ray.yarn.utils;

import com.sun.jersey.api.client.ClientHandlerException;
import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.ray.yarn.ApplicationMaster;

public class TimelineUtil {

  private static final Log logger = LogFactory.getLog(TimelineUtil.class);
  private static final String CONTAINER_ENTITY_GROUP_ID = "CONTAINERS";
  private static final String APPID_TIMELINE_FILTER_NAME = "appId";
  private static final String USER_TIMELINE_FILTER_NAME = "user";


  public static void publishContainerStartEvent(final TimelineClient timelineClient, final Container container,
      String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(container.getId().toString());
    entity.setEntityType(ApplicationMaster.DsEntity.DS_CONTAINER.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter(USER_TIMELINE_FILTER_NAME, ugi.getShortUserName());
    entity.addPrimaryFilter(APPID_TIMELINE_FILTER_NAME,
        container.getId().getApplicationAttemptId().getApplicationId()
            .toString());
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setEventType(ApplicationMaster.DsEvent.DS_CONTAINER_START.toString());
    event.addEventInfo("Node", container.getNodeId().toString());
    event.addEventInfo("Resources", container.getResource().toString());
    entity.addEvent(event);

    try {
      processTimelineResponseErrors(putContainerEntity(timelineClient, container.getId().getApplicationAttemptId(),
          entity, timelineClient.getConfig()));
    } catch (YarnException | IOException | ClientHandlerException e) {
      logger.error("Container start event could not be published for " + container
          .getId()
          .toString(), e);
    }
  }

  public static void publishContainerEndEvent(final TimelineClient timelineClient,
      ContainerStatus container,
      String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(container.getContainerId().toString());
    entity.setEntityType(ApplicationMaster.DsEntity.DS_CONTAINER.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter(USER_TIMELINE_FILTER_NAME, ugi.getShortUserName());
    entity.addPrimaryFilter(APPID_TIMELINE_FILTER_NAME,
        container.getContainerId().getApplicationAttemptId().getApplicationId()
            .toString());
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setEventType(ApplicationMaster.DsEvent.DS_CONTAINER_END.toString());
    event.addEventInfo("State", container.getState().name());
    event.addEventInfo("Exit Status", container.getExitStatus());
    entity.addEvent(event);
    try {
      processTimelineResponseErrors(putContainerEntity(timelineClient, container.getContainerId().getApplicationAttemptId(),
          entity, timelineClient.getConfig()));
    } catch (YarnException | IOException | ClientHandlerException e) {
      logger.error("Container end event could not be published for " + container
          .getContainerId()
          .toString(), e);
    }
  }

  public static TimelinePutResponse processTimelineResponseErrors(
      TimelinePutResponse response) {
    List<TimelinePutError> errors = response.getErrors();
    if (errors.size() == 0) {
      logger.debug("Timeline entities are successfully put");
    } else {
      for (TimelinePutResponse.TimelinePutError error : errors) {
        logger.error("Error when publishing entity [" + error.getEntityType() + ","
            + error
            .getEntityId() + "], server side error code: " + error
            .getErrorCode());
      }
    }
    return response;
  }

  private static TimelinePutResponse putContainerEntity(TimelineClient timelineClient, ApplicationAttemptId currAttemptId,
      TimelineEntity entity, Configuration yarnConf)
      throws YarnException, IOException {
    if (org.apache.hadoop.yarn.util.timeline.TimelineUtils.timelineServiceV1_5Enabled(yarnConf)) {
      TimelineEntityGroupId groupId = TimelineEntityGroupId.newInstance(currAttemptId.getApplicationId(), CONTAINER_ENTITY_GROUP_ID);
      return timelineClient.putEntities(currAttemptId, groupId, entity);
    } else {
      return timelineClient.putEntities(entity);
    }
  }
}
