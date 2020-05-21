const mongoose = require("mongoose");
const mongoosePaginate = require("mongoose-paginate-v2");
const Schema = mongoose.Schema;

const photoSchema = new Schema(
    {
        petMicrochip: {
            type: String,
            required: true,
            index: true,
        },
        petPhotoName: {
            type: String,
            default: "",
        },
        petPhotoData: {
            type: String,
            default: "",
        },
    },
    {
        collection: "photos",
        timestamps: {
            createdAt: "created_at",
            updatedAt: "updated_at",
        },
    }
);
photoSchema.plugin(mongoosePaginate);

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

const photo_local_model = local.model("photoSchema", photoSchema);
const photo_klikz_model = klikz.model("newphotoSchema", photoSchema);

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
            const photos = await photo_local_model.paginate(
                {},
                {
                    page: pageId,
                    limit: 20,
                    sort: {
                        _id: -1,
                    },
                }
            );

            console.log("thread " + thread + ": Processing Page " + process);

            if (!photos) {
                console.log("no more page");
                return;
            } else {
                for (let i = 0; i < photos.docs.length; i++) {
                    try {
                        const photo = photos.docs[i]._doc;
                        const new_photo = new photo_klikz_model(photo);
                        const result = await new_photo.save();
                        console.log(result.petMicrochip + "saved");
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
