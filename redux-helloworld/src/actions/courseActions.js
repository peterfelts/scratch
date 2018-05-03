import * as actionTypes from './actionTypes';

export function createCourse(course) {
    console.log("createCourse called.");
    return {type: actionTypes.CREATE_COURSE, course}
}