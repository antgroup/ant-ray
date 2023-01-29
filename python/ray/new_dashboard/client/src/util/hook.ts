import { get } from "lodash";
import { useState } from "react";

export const useFilter = <KeyType extends string>() => {
  const [filters, setFilters] = useState<{ key: KeyType; val: string }[]>([]);
  const changeFilter = (key: KeyType, val: string) => {
    const f = filters.find((e) => e.key === key);
    if (f) {
      f.val = val;
    } else {
      filters.push({ key, val });
    }
    setFilters([...filters]);
  };
  const filterFunc = (instance: { [key: string]: any }) => {
    return filters.every(
      (f) =>
        !f.val ||
        get(instance, f.key, "").toString().includes(f.val) ||
        f.val?.includes(get(instance, f.key, "").toString()),
    );
  };

  return {
    changeFilter,
    filterFunc,
  };
};

export const useSorter = (initialSortKey?: string) => {
  const [sorterKey, setSortKey] = useState(initialSortKey || "");
  const [desc, setOrderDesc] = useState(false);

  const sorterFunc = (
    instanceA: { [key: string]: any },
    instanceB: { [key: string]: any },
  ) => {
    if (!sorterKey) {
      return 0;
    }

    let [a, b] = [instanceA, instanceB];
    if (desc) {
      [b, a] = [instanceA, instanceB];
    }

    if (!get(a, sorterKey)) {
      return -1;
    }

    if (!get(b, sorterKey)) {
      return 1;
    }

    return get(a, sorterKey) > get(b, sorterKey) ? 1 : -1;
  };

  return {
    sorterFunc,
    setSortKey,
    setOrderDesc,
    sorterKey,
    desc,
  };
};

export const usePage = (
  init: { page?: number; pageSize?: number; total?: number } = {},
) => {
  const [page, setPage] = useState({
    page: 1,
    pageSize: 10,
    total: 1,
    ...init,
  });

  return {
    page: page.page,
    pageSize: page.pageSize,
    startIndex: page.pageSize * (page.page - 1),
    endIndex: page.pageSize * page.page,
    total: page.total,
    totalPage: Math.ceil(page.total / page.pageSize),
    setPage: (val: number) => setPage({ ...page, page: val }),
    setTotal: (val: number) => setPage({ ...page, total: val }),
    setPageSize: (val: number) => setPage({ ...page, pageSize: val }),
  };
};
