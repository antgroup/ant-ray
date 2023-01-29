import { makeStyles } from "@material-ui/core";
import React, { useEffect, useState } from "react";
import EventTable from "../../components/EventTable";
import TitleCard from "../../components/TitleCard";
import { getRayConfig } from "../../service/cluster";
import { PageTitle } from "../../util/func";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
}));

const Events = () => {
  const classes = useStyle();
  const [slsUrl, setSls] = useState<string>();
  useEffect(() => {
    getRayConfig().then((res) => {
      const urlObj = res?.data?.data?.config?.slsUrl;
      if (typeof urlObj === "object") {
        setSls(urlObj.event);
      }
    });
  }, []);

  return (
    <div className={classes.root}>
      <PageTitle title="Event" />
      <TitleCard title="Event">
        <EventTable slsUrl={slsUrl} />
      </TitleCard>
    </div>
  );
};

export default Events;
