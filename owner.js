const mongoose = require("mongoose");
const mongoosePaginate = require("mongoose-paginate-v2");
const Schema = mongoose.Schema;

const ownerSchema = new Schema(
    {
        email: {
            type: String,
            required: true,
            index: true,
        },
        ownerName: {
            type: String,
            default: "",
        },
        ownerPhone1: {
            type: String,
            default: "",
        },
        ownerPhone2: {
            type: String,
            default: "",
        },
        ownerPhone3: {
            type: String,
            default: "",
        },
        ownerPhone4: {
            type: String,
            default: "",
        },
        ownerPhone5: {
            type: String,
            default: "",
        },
        ownerPhone6: {
            type: String,
            default: "",
        },
        ownerPhone7: {
            type: String,
            default: "",
        },
        ownerAddress1: {
            type: String,
            default: "",
        },
        ownerAddress2: {
            type: String,
            default: "",
        },
        ownerCity: {
            type: String,
            default: "",
        },
        ownerState: {
            type: String,
            default: "",
        },
        ownerZip: {
            type: String,
            default: "",
        },
        ownerCountry: {
            type: String,
            default: "US",
        },
        ownerSecContact: {
            type: String,
            default: "",
        },
        ownerNote: {
            type: String,
            default: "",
        },
        registered_at: {
            type: String,
            default: "01/01/2001",
        },
    },
    {
        collection: "owners",
        timestamps: {
            createdAt: "created_at",
            updatedAt: "updated_at",
        },
    }
);
ownerSchema.plugin(mongoosePaginate);

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

const owner_local_model = local.model("ownerSchema", ownerSchema);
const owner_klikz_model = klikz.model("newownerSchema", ownerSchema);

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
            const owners = await owner_local_model.paginate(
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

            if (!owners) {
                console.log("no more page");
                return;
            } else {
                for (let i = 0; i < owners.docs.length; i++) {
                    try {
                        const owner = owners.docs[i]._doc;
                        const new_owner = new owner_klikz_model(owner);
                        const result = await new_owner.save();
                        console.log(result.email + "saved");
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
