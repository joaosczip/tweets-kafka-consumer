#! /bin/node

import { Kafka } from "kafkajs";
import chalk from "chalk";
import chalkTable from "chalk-table";
import { uniq, flatten, append } from "ramda";

const TOPIC = "twitter-tweets";
const GROUP_ID = "tweets-consumers";

const mountTable = (wordsWithQuantity) => {
  const options = {
    leftPad: 2,
    columns: [
      { field: "position", name: chalk.cyan("Position") },
      { field: "word", name: chalk.cyan("Word") },
      { field: "quantity", name: chalk.magenta("Quantity") },
    ],
  };

  return chalkTable(options, wordsWithQuantity);
};

const getUniqWords = (tweet, words) =>
  uniq(
    flatten(
      append(
        tweet
          .split(" ")
          .map((word) => word.replace(/[-,.\\/()0-9@!?RT" "]/g, "").trim())
          .filter(String),
        words
      )
    )
  );

const sort = (inputArr, key) => {
  let n = inputArr.length;

  for (let i = 0; i < n; i++) {
    let min = i;
    for (let j = i + 1; j < n; j++) {
      if (inputArr[j][key] < inputArr[min][key]) {
        min = j;
      }
    }
    if (min != i) {
      let tmp = inputArr[i];
      inputArr[i] = inputArr[min];
      inputArr[min] = tmp;
    }
  }
  return inputArr;
};

const mapWordsWithQuantity = (quantity) => {
  const mapped = Object.keys(quantity)
    .map((q) => ({
      word: q,
      quantity: quantity[q],
    }))
    .slice(0, 10);

  return sort(mapped, "quantity")
    .reverse()
    .map((value, index) => ({ ...value, position: index + 1 }));
};

const processMessage = (words, quantity) => {
  return (message) => {
    const { text } = JSON.parse(message.value.toString());
    const tweet = String(text);

    const uniqWords = getUniqWords(tweet, words);
    uniqWords.map((word) => {
      quantity[word] = quantity[word] ? quantity[word] + 1 : 1;
    });

    const wordsWithQuantity = mapWordsWithQuantity(quantity);
    const table = mountTable(wordsWithQuantity);
    console.log(table);
  };
};

const init = async () => {
  const kafka = new Kafka({
    brokers: ["localhost:9092"],
  });

  const consumer = kafka.consumer({
    groupId: GROUP_ID,
  });

  await consumer.connect();
  await consumer.subscribe({
    topic: TOPIC,
  });

  const words = [];
  const quantity = {};

  await consumer.run({
    eachBatch: ({ batch }) => {
      try {
        batch.messages.forEach(processMessage(words, quantity));
      } catch (error) {
        console.error("error", error);
      }
    },
  });
};

init();
