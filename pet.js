const mongoose = require("mongoose");
const mongoosePaginate = require("mongoose-paginate-v2");
const Schema = mongoose.Schema;

const petSchema = new Schema(
    {
        microchip: {
            type: String,
            index: true,
        },
        petName: {
            type: String,
            default: "",
        },
        petSpecies: {
            type: String,
            default: "dog",
        },
        petBreed: {
            type: String,
            default: "",
        },
        petColor: {
            type: String,
            default: "",
        },
        petGender: {
            type: String,
            default: "Male",
        },
        petBirth: {
            type: String,
            default: "01/01/2001",
        },
        specialNeeds: {
            type: String,
            default: "",
        },
        vetInfo: {
            type: String,
            default: "",
        },
        dateRV: {
            type: String,
            default: "",
        },
        implantedCompany: {
            type: String,
            default: "",
        },
        email: {
            type: String,
            default: "",
        },
        ownerId: {
            type: String,
            default: "",
        },
        photoPath: {
            type: String,
            default: "",
        },
        ownerName: {
            type: String,
            default: "",
        },
        membership: {
            type: String,
            default: "platinum",
        },
        registered_at: {
            type: String,
            default: "01/01/2001",
        },
    },
    {
        collection: "pets",
        timestamps: {
            createdAt: "created_at",
            updatedAt: "updated_at",
        },
    }
);
petSchema.plugin(mongoosePaginate);

const local = mongoose.createConnection(
    "mongodb://localhost:27017/savethislife?readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false",
    {
        useNewUrlParser: true,
        useUnifiedTopology: true,
    }
);
const klikz = mongoose.createConnection(
    "mongodb+srv://stl:stl@klikzus-p8kcd.mongodb.net/savethislife?retryWrites=true&w=majority",
    {
        useNewUrlParser: true,
        useUnifiedTopology: true,
    }
);

const pet_local_model = local.model("petSchema", petSchema);
const pet_klikz_model = klikz.model("newpetSchema", petSchema);

const thread_num = 10;
const max_page_id = 100000;
const start_page_id = 1;

var pageId = start_page_id;

local.once("open", function () {
    console.log("MongoDB Local Database connection established Successfully.");
    klikz.once("open", function () {
        console.log(
            "MongoDB Klikz Database connection established Successfully."
        );

        for (let thread = 1; thread < thread_num + 1; thread++) {
            setTimeout(() => {
                start_thread(thread);
            }, thread * 3000);
        }
    });
});

async function start_thread(thread) {
    while (pageId < max_page_id) {
        const process = pageId++;

        try {
            const pets = await pet_local_model.paginate(
                {},
                {
                    page: process,
                    limit: 20,
                    sort: {
                        _id: -1,
                    },
                }
            );

            console.log("thread " + thread + ": Processing Page " + process);

            if (!pets) {
                console.log("no more page");
                return;
            } else {
                for (let i = 0; i < pets.docs.length; i++) {
                    try {
                        const pet = pets.docs[i]._doc;
                        const new_pet = new pet_klikz_model(pet);
                        const result = await new_pet.save();
                        console.log(result.microchip + "saved");
                    } catch (error) {
                        console.log(error);
                        continue;
                    }
                }
            }
        } catch (error) {
            console.log(error);
        }
    }
}
