import {
  connectMongoConnector,
  getDbWrapper,
} from "../common/db/mongo-wrapper";
import { Collection, InsertOneWriteOpResult, ObjectId } from "mongodb";
import { Gender } from "../common/types/db-models/user";

async function cleanCollections() {
  const segmentsCollection: Collection = await (
    await getDbWrapper()
  ).getCollection("segments");
  const usersCollection: Collection = await (
    await getDbWrapper()
  ).getCollection("users");

  console.log("cleaning segment collection");
  await segmentsCollection.deleteMany({});
  console.log("cleaning user collection");
  await usersCollection.deleteMany({});
  console.log("Done cleaning");
}

function randomNumber(min: number, max: number): number {
  return Math.round(Math.random() * (max - min) + min);
}

async function createUsers() {
  const incomes = [
    {
      value: 5000,
      type: "monthly",
    },
    {
      value: 7500,
      type: "monthly",
    },
    {
      value: 5000,
      type: "monthly",
    },
    {
      value: 7500,
      type: "monthly",
    },
    {
      value: 5000,
      type: "monthly",
    },
    {
      value: 7500,
      type: "monthly",
    },
    {
      value: 5000,
      type: "monthly",
    },
    {
      value: 7500,
      type: "monthly",
    },
    {
      value: 5000,
      type: "monthly",
    },
    {
      value: 7500,
      type: "monthly",
    },
    {
      value: 5000,
      type: "monthly",
    },
    {
      value: 7500,
      type: "monthly",
    },
    {
      value: 5000,
      type: "monthly",
    },
    {
      value: 7500,
      type: "monthly",
    },
    {
      value: 10000,
      type: "monthly",
    },
    {
      value: 15000,
      type: "monthly",
    },
    {
      value: 10000,
      type: "monthly",
    },
    {
      value: 15000,
      type: "monthly",
    },
    {
      value: 10000,
      type: "monthly",
    },
    {
      value: 15000,
      type: "monthly",
    },
    {
      value: 10000,
      type: "monthly",
    },
    {
      value: 15000,
      type: "monthly",
    },
    {
      value: 25000,
      type: "monthly",
    },
    {
      value: 25000,
      type: "monthly",
    },
    {
      value: 35000,
      type: "monthly",
    },
    {
      value: 35000,
      type: "monthly",
    },
    {
      value: 75000,
      type: "yearly",
    },
    {
      value: 100000,
      type: "yearly",
    },
    {
      value: 75000,
      type: "yearly",
    },
    {
      value: 100000,
      type: "yearly",
    },
    {
      value: 75000,
      type: "yearly",
    },
    {
      value: 100000,
      type: "yearly",
    },
    {
      value: 75000,
      type: "yearly",
    },
    {
      value: 100000,
      type: "yearly",
    },
    {
      value: 75000,
      type: "yearly",
    },
    {
      value: 100000,
      type: "yearly",
    },
    {
      value: 75000,
      type: "yearly",
    },
    {
      value: 100000,
      type: "yearly",
    },
    {
      value: 75000,
      type: "yearly",
    },
    {
      value: 100000,
      type: "yearly",
    },
    {
      value: 150000,
      type: "yearly",
    },
    {
      value: 150000,
      type: "yearly",
    },
    {
      value: 150000,
      type: "yearly",
    },
    {
      value: 150000,
      type: "yearly",
    },
    {
      value: 150000,
      type: "yearly",
    },
    {
      value: 250000,
      type: "yearly",
    },
    {
      value: 250000,
      type: "yearly",
    },
    {
      value: 500000,
      type: "yearly",
    },
  ];
  const genders = [
    "Female",
    "Male",
    "Female",
    "Female",
    "Female",
    "Female",
    "Female",
    "Male",
    "Male",
    "Male",
    "Male",
    "Male",
    "Female",
    "Female",
    "Female",
    "Female",
    "Female",
    "Female",
    "Male",
    "Male",
    "Male",
    "Male",
    "Male",
    "Male",
    "Other",
  ];

  const genderLen: number = genders.length - 1;
  const incomeLen: number = incomes.length - 1;
  const usersCollection: Collection = await (
    await getDbWrapper()
  ).getCollection("users");

  const totalUsers = 100000;
  for (let i = 0; i < totalUsers; i++) {
    if (i % 1000 === 0) {
      console.log(`starting ${i}/${totalUsers}`);
    }
    const gender = randomNumber(0, genderLen);
    const income = randomNumber(0, incomeLen);
    const age = randomNumber(18, 65);
    const user = {
      age: age,
      gender: genders[gender],
      income_level: incomes[income].value,
      income_type: incomes[income].type,
    };

    await usersCollection.insertOne(user);
  }
}

async function createSegments(): Promise<void> {
  const qualities: string[] = [
    "Smart",
    "Ambitious",
    "Lazy",
    "Stupid",
    "Fat",
    "Skinny",
    "Dexterous",
    "Nimble",
    "Agile",
    "Sincere",
    "Honest",
    "Understanding",
    "Loyal",
    "Truthful",
    "Trustworthy",
    "Intelligent",
    "Dependable",
    "Open-Minded",
    "Wise",
    "Reliable",
    "Mature",
    "Friendly",
    "King-Hearted",
    "Warm",
    "Happy",
    "Good-Natured",
    "Selfish",
    "Honorable",
    "Respectful",
    "Cheerful",
    "Responsible",
    "Angry",
    "Furious",
    "Evil",
    "Helpful",
    "Brilliant",
  ];

  const groups: string[] = [
    "Kids",
    "Teenagers",
    "Adults",
    "People",
    "Women",
    "Investigators",
    "Men",
    "Detectives",
    "Chef's",
    "Cooks",
    "Policemen",
    "Firefighters",
    "Politicians",
    "Nanny's",
    "Mommies",
    "Daddies",
    "Grandparents",
    "Aunts & Uncles",
    "Retailers",
    "Developers",
    "Cleaners",
  ];
  const groupLen: number = groups.length - 1;
  const qualitiesLen: number = qualities.length - 1;
  const segmentsCollection: Collection = await (
    await getDbWrapper()
  ).getCollection("segments");
  const usersCollection: Collection = await (
    await getDbWrapper()
  ).getCollection("users");

  const totalSegments = 2500;
  const maxSegmentSize = 5000;
  const nameSet = new Set();

  for (let i = 0; i < totalSegments; i++) {
    if (i % 100 === 0) {
      console.log(`starting ${i}/${totalSegments}`);
    }
    const groupIndex = randomNumber(0, groupLen - 1);
    const qualityIndex = randomNumber(0, qualitiesLen - 1);

    let segmentName: string = `${qualities[qualityIndex]} ${groups[groupIndex]}`;
    while (nameSet.has(segmentName)) {
      const qualityIndex2 = randomNumber(0, qualitiesLen - 1);
      segmentName = `${qualities[qualityIndex2]} ${segmentName}`;
    }

    nameSet.add(segmentName);
    const inserted: InsertOneWriteOpResult<any> =
      await segmentsCollection.insertOne({
        name: segmentName,
      });

    const segmentSize = randomNumber(0, maxSegmentSize);

    const userSelectionPipeline: any[] = [
      {
        $sample: {
          size: segmentSize,
        },
      },
      {
        $project: {
          _id: 1,
        },
      },
    ];

    if (segmentName.endsWith("Daddies") || segmentName.endsWith("Men")) {
      userSelectionPipeline.unshift({
        $match: {
          gender: Gender.Male,
        },
      });
    }
    if (segmentName.endsWith("Mommies") || segmentName.endsWith("Women")) {
      userSelectionPipeline.unshift({
        $match: {
          gender: Gender.Female,
        },
      });
    }
    const users: { _id: ObjectId }[] = await usersCollection
      .aggregate(userSelectionPipeline)
      .toArray();

    await usersCollection.updateMany(
      {
        _id: { $in: users.map((v) => v._id) },
      },
      {
        $addToSet: {
          segment_ids: inserted.insertedId,
        },
      }
    );
    console.log(`found ${users.length} users for ${segmentName}`);
  }
}

connectMongoConnector().then(async () => {
  try {
    // await cleanCollections();
    console.log(`creating users`);
    await createUsers();
    console.log(`creating users`);
    await createSegments();
  } catch (e) {
    console.log(e?.message || e);
  }
});
