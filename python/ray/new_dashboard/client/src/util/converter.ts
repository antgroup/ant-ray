import dayjs from 'dayjs';

export const memoryConverter = (bytes: number) => {
  if (bytes < 1024) {
    return `${bytes}KB`;
  }

  if (bytes < 1024 ** 2) {
    return `${(bytes / 1024 ** 1).toFixed(1)}KB`;
  }

  if (bytes < 1024 ** 3) {
    return `${(bytes / 1024 ** 2).toFixed(1)}MB`;
  }

  if (bytes < 1024 ** 4) {
    return `${(bytes / 1024 ** 3).toFixed(1)}GB`;
  }

  if (bytes < 1024 ** 5) {
    return `${(bytes / 1024 ** 4).toFixed(1)}TB`;
  }

  if (bytes < 1024 ** 6) {
    return `${(bytes / 1024 ** 5).toFixed(1)}TB`;
  }

  return "";
};

export const formatTime = (timeValue: string | number, formatStr: string = 'YYYY/MM/DD HH:mm:ss') => {
  const num = Number(timeValue);
  let time;

  if (Number.isNaN(num)) {
    time = dayjs(timeValue);
  } else if (Math.log10(num) < 10) {
    time = dayjs(num * 1000)
  } else {
    time = dayjs(num)
  }

  return time.format(formatStr)
}
