import React from 'react';
import { ArrowDownward, ArrowUpward } from "@material-ui/icons";
import { Chip } from "@material-ui/core";

export const SortButton = ({
  target,
  title,
  sorterKey,
  desc,
  setSortKey,
  setOrderDesc,
}: {
  target: string;
  title: string;
  sorterKey: string;
  desc: boolean;
  setSortKey: (str: string) => void;
  setOrderDesc: (desc: boolean) => void;
}) => {
  const isSorting = target === sorterKey;
  const onClick = () => {
    if (isSorting) {
      if (desc) {
        setOrderDesc(false);
        setSortKey("");
      } else {
        setOrderDesc(true);
      }
    } else {
      setOrderDesc(false);
      setSortKey(target);
    }
  };
  return (
    <Chip
      size="small"
      onClick={onClick}
      label={title}
      icon={isSorting ? !desc ? <ArrowUpward /> : <ArrowDownward /> : undefined}
      color={isSorting ? "primary" : "default"}
    />
  );
};